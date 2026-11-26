#!/usr/bin/env python3
"""Check a public source tree without emitting matched content or private inputs."""

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path


HOME_PATH = re.compile(r"/Users/([A-Za-z0-9_.-]+)(?:/[^\s\"'<>`;,)}\]]*)?")
GROUP_ID = re.compile(r"(?<![0-9])[0-9]{10,}@chatroom\b")
MEMBER_ID = re.compile(r"\bwxid_[A-Za-z0-9_-]{10,}\b")
SECRET = re.compile(
    r"\b(?:sk-(?:proj-|ant-)?[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9]{30,}"
    r"|github_pat_[A-Za-z0-9_]{30,}|AKIA[A-Z0-9]{16})\b"
    r"|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"
)
PRIVATE_IMPL = re.compile(r"(?:local_store|db_poller|sqlcipher|keyextract)", re.I)
CODE_SUFFIXES = {".go", ".py", ".c", ".cc", ".cpp", ".h", ".rs", ".swift", ".sh", ".js", ".ts", ".m", ".mm", ".java", ".cs", ".so", ".dylib", ".dll", ".a"}
PLACEHOLDER = re.compile(r"(?:example|synthetic|placeholder|demo|redacted|your[_-]?(?:user|key|name))", re.I)
PUBLIC_VALUES = {"localhost", "127.0.0.1", "wechat", "genericwechat", "jinx", "hermes", "default", "independent_run"}
SENSITIVE_KEYS = {
    "values", "botnames", "selfsenderids", "blockedsenderids", "groupwhitelist", "identityvalues",
    "groupprofiles", "groupotherbots", "target", "targetgroupid", "groupid",
    "memberid", "senderid", "brandtext", "brand", "identity", "displayname",
    "name", "aliases", "decisioncontext", "sensitivecriteria", "noisecriteria",
    "safecriteria", "promokeywords", "knowledge", "knowledgesources", "knowledgepaths",
    "knowledgefiles", "prompt", "promptprefix", "soulpath", "apikey", "token", "identities",
    "secret", "password", "clientsecret", "supportgroupid", "selfsenderid",
    "communicationfile", "instructionsfile", "documentfiles", "releasefiles", "codefactsfile",
    "memoryuser", "vectorcollection", "application", "bundleid", "calendartitle", "sourcetitle",
    "titleprefix", "markerprefix", "apppath", "binpath", "profilesdir",
}


@dataclass(frozen=True)
class Finding:
    category: str
    path: str
    line: int
    count: int = 1


def synthetic(value):
    numeric = value.removesuffix("@chatroom")
    return bool(PLACEHOLDER.search(value)) or len(set(numeric)) == 1 or numeric.startswith("1234567890")


def matches(content):
    for match in HOME_PATH.finditer(content):
        if match.group(1).lower() not in {"user", "username", "example", "demo", "operator", "shared"} and not PLACEHOLDER.search(match.group(1)):
            yield "user_home", match
    for category, pattern in (("group_identity", GROUP_ID), ("member_identity", MEMBER_ID), ("secret_prefix", SECRET)):
        for match in pattern.finditer(content):
            if category == "secret_prefix" or not synthetic(match.group()):
                yield category, match


def baseline_values(data):
    values = set()

    def visit(value, sensitive=False):
        if isinstance(value, dict):
            for key, child in value.items():
                if GROUP_ID.fullmatch(key):
                    values.add(key)
                    if len(key.removesuffix("@chatroom")) >= 10:
                        values.add(key.removesuffix("@chatroom"))
                normalized = re.sub(r"[^a-z]", "", key.lower())
                visit(child, sensitive or normalized in SENSITIVE_KEYS)
        elif isinstance(value, list):
            for child in value:
                visit(child, sensitive)
        elif isinstance(value, int) and not isinstance(value, bool) and sensitive and value >= 10**9:
            values.add(str(value))
        elif isinstance(value, str):
            for category, match in matches(value):
                values.add(match.group())
                if category == "group_identity":
                    values.add(match.group().removesuffix("@chatroom"))
                if category == "user_home":
                    values.add("/Users/" + match.group(1))
            if sensitive and (len(value.strip()) >= 4 or (len(value.strip()) >= 2 and not value.isascii())):
                if value.lower() not in PUBLIC_VALUES:
                    values.add(value)

    visit(data, isinstance(data, list))
    return frozenset(values)


def load_baseline(path):
    if path.is_symlink() or not path.exists():
        raise ValueError("invalid baseline")
    values = set()
    files = sorted(path.rglob("*")) if path.is_dir() else [path]
    for source in files:
        if source.is_symlink() or not source.is_file() or ".example." in source.name:
            continue
        content = source.read_bytes().decode("utf-8", errors="replace")
        try:
            data = json.loads(content)
        except (ValueError, RecursionError):
            if ".json" in source.name:
                raise ValueError("invalid baseline") from None
            data = content
        values.update(baseline_values(data))
    if not values:
        raise ValueError("empty baseline")
    return frozenset(values)


def safe_path(path, baseline):
    for value in sorted(baseline, key=len, reverse=True):
        path = path.replace(value, "[redacted]")
    for _, match in sorted(matches(path), key=lambda item: item[1].start(), reverse=True):
        path = path[:match.start()] + "[redacted]" + path[match.end():]
    return json.dumps(path, ensure_ascii=True)


def inspect_file(path, content, baseline=frozenset()):
    findings = []
    public_path = safe_path(path, baseline)
    if Path(path).suffix.lower() in CODE_SUFFIXES and PRIVATE_IMPL.search(path):
        findings.append(Finding("private_implementation", public_path, 0))
    text = content.decode("utf-8", errors="replace")
    for line_number, line in enumerate((path + "\n" + text).splitlines()):
        counts = {}
        for category, _ in matches(line):
            counts[category] = counts.get(category, 0) + 1
        count = sum(line.count(value) for value in baseline if value in line)
        if count:
            counts["private_baseline"] = count
        for category, count in sorted(counts.items()):
            findings.append(Finding(category, public_path, line_number, count))
    return findings


def scan_tree(root, baseline=frozenset()):
    findings = []
    for source in sorted(root.rglob("*")):
        relative = source.relative_to(root)
        if ".git" in relative.parts:
            continue
        public_path = safe_path(relative.as_posix(), baseline)
        if source.is_symlink():
            findings.append(Finding("boundary_symlink", public_path, 0))
        elif source.is_file():
            try:
                findings.extend(inspect_file(relative.as_posix(), source.read_bytes(), baseline))
            except OSError:
                findings.append(Finding("unreadable_public_file", public_path, 0))
    return findings


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("public_tree", nargs="?", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--private-baseline", type=Path, help="explicit local config, backup file or directory; never printed")
    args = parser.parse_args(argv)
    baseline = frozenset()
    if args.private_baseline is not None:
        try:
            if not args.private_baseline.exists():
                raise ValueError("missing baseline")
            baseline = load_baseline(args.private_baseline)
        except (OSError, ValueError, RecursionError):
            print("category=baseline_unreadable path=- line=0 count=1")
            return 2
    if not args.public_tree.is_dir():
        print("category=invalid_public_tree path=- line=0 count=1")
        return 2
    try:
        findings = scan_tree(args.public_tree, baseline)
    except OSError:
        print("category=unreadable_public_tree path=- line=0 count=1")
        return 2
    for finding in findings:
        print(f"category={finding.category} path={finding.path} line={finding.line} count={finding.count}")
    print(f"category=total path=- line=0 count={sum(finding.count for finding in findings)}")
    return int(bool(findings))


if __name__ == "__main__":
    raise SystemExit(main())
