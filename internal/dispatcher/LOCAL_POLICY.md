# Operator local policy

`Options.LocalPolicyPath` selects a local JSON policy. An empty option falls back
to `JINX_LOCAL_POLICY`; with neither, business support and reminders are disabled.
The legacy `cmd/dispatcher` also accepts `-local-policy`.

Keep the policy outside the public checkout. The policy and its optional
`communication_file` and `support.instructions_file` must be regular 0600 files;
symlinks, unknown schema fields, trailing JSON and unsupported versions fail
startup. Loading performs reads only. Restart to apply policy changes.

`local-policy.example.json` contains synthetic identities with support disabled.
Container identifiers support WeChat numeric `<digits>@chatroom` IDs as well as
namespaced `bilibili:<local>` and `web:<local>` IDs. Each local identity specifies
an isolated profile, memory user and vector collection. For WeChat groups without
a local override, memory uses `group:<group ID>` and collections use
`group_<numeric group ID>`; for non-WeChat containers without an override, memory
uses `container:<container ID>` and collections use
`container_<sanitized container ID>`. Existing deployments can retain their
original identities with explicit local overrides.

`communication_mode` selects how `communication_file` is applied: `"append"` (the
default when omitted) appends the private guidance after the public rules, while
`"replace"` keeps the public core safety/boundary rules and replaces the default
conversational guidance with `communication_file` (and requires a non-empty
`communication_file`). Similarly, `support.instructions_mode` supports `"append"`
(default) or `"replace"` (retaining the core support safety/evidence boundary while
replacing the built-in support boilerplate with `support.instructions_file`).

Optional `scenarios.<id>` entries bind scenario presets or custom scenarios to a
private `0600` `prompt_file` (`prompt_mode`: `"append"` or `"replace"`), scenario
`knowledge_files`, default `skills`/`tools` logical IDs, and an optional
`max_reply_runes` cap. Optional `knowledge_bases.<id>` entries map logical KB IDs
referenced by public `group_rules[*].knowledge_bases` to explicit absolute file
lists and optional per-file `max_bytes` (`256..65536`); unknown KB references fail
closed during configuration validation. All private paths remain strictly inside the
local policy and never appear in `/api/config` or trace snapshots.

To authorize support, set `support.enabled`, `group_id` and exactly one verified
`self_sender_ids` entry, and add a matching identity. The public group rule must
also explicitly enable `support_mode`, set `support_since_ms`, and use the same
self identity and profile. Support replies remain in their source group.
Mention-only and promotional routes cannot be combined with support. API edits
cannot remove the local support route or downgrade it to ordinary replies;
`enabled:false` can still stop the route.

Knowledge is read only from absolute paths explicitly listed in
`support.knowledge_files`, `document_files` and `release_files`. No home-directory,
skill-directory or sibling-product discovery occurs. Missing files produce a
knowledge gap. `code_facts_file` permits one audited, credential-free source file
paired with explicit regex extraction rules in `support.code_fact_rules` (each
entry specifying `"label"` and a single-capture-group `"pattern"`); only extracted
values are emitted, never source snippets, and missing rules fail closed. Dated release
answers are checked against the allowed release and document files.

Reminders require a separate explicit `support.reminders.enabled` plus
`app_path`, `application`, `bundle_id`, `calendar_title`, `source_title`,
`source_type:2`, `require_cloud_verified:true`, `title_prefix` and `marker_prefix`.
The configured app must implement the existing request-file protocol and return
matching app, bundle and verified calendar metadata. Preserve an existing marker
prefix when migrating task mappings. Neither policy loading nor dispatcher
construction launches the app. Task synchronization may write only after the
existing evidence, conservative classification and dry-run gates permit it.

Local policy fields and file locations are absent from `Config` JSON, config
readback, ETags and retained config snapshots. Trusted communication text is
delivered to the configured backend as part of the prompt. Ordinary group rules
remain visible through the existing local configuration API.

Validation uses synthetic temporary knowledge, product source and reminders
fixtures. On macOS installations requiring the external linker:

```sh
go test -race -ldflags=-linkmode=external ./internal/dispatcher
go vet ./internal/dispatcher
```
