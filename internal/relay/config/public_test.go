package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func publicManager(t *testing.T) *Manager {
	t.Helper()
	manager, err := NewManager(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func publicValue(snapshot PublicSnapshot) PublicConfig {
	return PublicConfig{GroupControl: snapshot.GroupControl, Filters: snapshot.Filters}
}

func TestPublicPreservesPrivateConfigAndCopies(t *testing.T) {
	manager := publicManager(t)
	private := manager.Get()
	private.Server.ListenAddr = "127.0.0.1:18080"
	private.Upstream.WSURL = "ws://synthetic.invalid/synthetic-private-token"
	if err := manager.Update(private); err != nil {
		t.Fatal(err)
	}
	before := manager.GetPublic()
	public := publicValue(before)
	public.GroupControl.GroupAliases = map[string]string{"synthetic@chatroom": "Synthetic"}
	public.Filters.AllowedTypes = []int{3, 244813135921}
	after, err := manager.CompareAndSwap(before.Revision, public)
	if err != nil || after.Revision == before.Revision {
		t.Fatalf("CAS failed: %v", err)
	}
	public.GroupControl.GroupAliases["synthetic@chatroom"] = "mutated"
	public.Filters.AllowedTypes[0] = 1
	after.GroupControl.GroupAliases["synthetic@chatroom"] = "mutated snapshot"
	after.Filters.AllowedTypes[0] = 49
	current := manager.Get()
	if current.GroupControl.GroupAliases["synthetic@chatroom"] != "Synthetic" || current.Filters.AllowedTypes[0] != 3 {
		t.Fatal("public snapshots share mutable state")
	}
	if current.Server != private.Server || current.Upstream != private.Upstream {
		t.Fatal("public update changed private config")
	}
	data, err := os.ReadFile(manager.filePath)
	if err != nil {
		t.Fatal(err)
	}
	var saved Config
	if json.Unmarshal(data, &saved) != nil || !reflect.DeepEqual(saved, current) {
		t.Fatal("disk and memory differ")
	}
	info, err := os.Stat(manager.filePath)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("config file must remain private")
	}
	encoded, _ := json.Marshal(manager.GetPublic())
	for _, forbidden := range []string{"server", "upstream", "local_db", "keys_file", "synthetic-private-token"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("public snapshot leaked %s", forbidden)
		}
	}
}

func TestPublicNullAndEmptyRoundTrip(t *testing.T) {
	manager := publicManager(t)
	initial := manager.GetPublic()
	if initial.GroupControl.MonitorAll || len(initial.GroupControl.ExposedGroups) != 0 || len(initial.GroupControl.MonitoredGroups) != 0 {
		t.Fatal("new configuration must not implicitly monitor or expose all groups")
	}
	for _, empty := range []bool{false, true} {
		public := PublicConfig{}
		if empty {
			public.GroupControl.MonitoredGroups = []string{}
			public.GroupControl.ExposedGroups = []string{}
			public.GroupControl.GroupAliases = map[string]string{}
			public.Filters = FilterConfig{KeywordBlocklist: []string{}, RegexBlocklist: []string{}, SenderBlocklist: []string{}, AllowedTypes: []int{}}
		}
		if _, err := manager.UpdatePublic(public); err != nil {
			t.Fatal(err)
		}
		if got := publicValue(manager.GetPublic()); !reflect.DeepEqual(got, public) {
			t.Fatalf("nil/empty changed: %#v", got)
		}
		reloaded, err := NewManager(manager.filePath)
		if err != nil || !reflect.DeepEqual(publicValue(reloaded.GetPublic()), public) {
			t.Fatalf("reload lost nil/empty: %v", err)
		}
	}
}

func TestPublicCASConcurrentWriters(t *testing.T) {
	manager := publicManager(t)
	before := manager.GetPublic()
	start := make(chan struct{})
	results := make(chan error, 16)
	var workers sync.WaitGroup
	for writer := 0; writer < cap(results); writer++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, err := manager.CompareAndSwap(before.Revision, publicValue(before))
			results <- err
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrRevisionConflict) {
			t.Fatal(err)
		}
	}
	if successes != 1 {
		t.Fatalf("expected one CAS winner, got %d", successes)
	}
}

func TestPublicLegacyUpdatesInvalidateCAS(t *testing.T) {
	manager := publicManager(t)
	initial := manager.Get()
	before := manager.GetPublic()
	privateOnly := manager.Get()
	privateOnly.Upstream.WSURL = "ws://synthetic.invalid/changed"
	if err := manager.Update(privateOnly); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CompareAndSwap(before.Revision, publicValue(before)); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("legacy private update did not invalidate CAS: %v", err)
	}
	if err := manager.Update(initial); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CompareAndSwap(before.Revision, publicValue(before)); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("ABA update did not invalidate CAS: %v", err)
	}
}

func TestPublicValidationAndWriteFailureAreAtomic(t *testing.T) {
	manager := publicManager(t)
	before := manager.GetPublic()
	for name, change := range map[string]func(*PublicConfig){
		"regex":            func(public *PublicConfig) { public.Filters.RegexBlocklist = []string{"["} },
		"zero type":        func(public *PublicConfig) { public.Filters.AllowedTypes = []int{0} },
		"negative type":    func(public *PublicConfig) { public.Filters.AllowedTypes = []int{-1} },
		"group ID":         func(public *PublicConfig) { public.GroupControl.ExposedGroups = []string{"bad@@chatroom"} },
		"partial wildcard": func(public *PublicConfig) { public.GroupControl.MonitoredGroups = []string{"room*"} },
		"glob":             func(public *PublicConfig) { public.GroupControl.ExposedGroups = []string{"room?"} },
		"alias ID":         func(public *PublicConfig) { public.GroupControl.GroupAliases = map[string]string{"name": "Alias"} },
		"empty entry":      func(public *PublicConfig) { public.Filters.KeywordBlocklist = []string{" "} },
		"long entry":       func(public *PublicConfig) { public.Filters.KeywordBlocklist = []string{strings.Repeat("x", 1025)} },
		"long group":       func(public *PublicConfig) { public.GroupControl.ExposedGroups = []string{strings.Repeat("x", 257)} },
		"long regex":       func(public *PublicConfig) { public.Filters.RegexBlocklist = []string{strings.Repeat("x", 4097)} },
		"too many entries": func(public *PublicConfig) { public.Filters.AllowedTypes = make([]int, 1025) },
	} {
		t.Run(name, func(t *testing.T) {
			public := publicValue(manager.GetPublic())
			change(&public)
			if _, err := manager.CompareAndSwap(before.Revision, public); !errors.Is(err, ErrInvalidPublic) {
				t.Fatalf("expected validation failure: %v", err)
			}
			if !reflect.DeepEqual(before, manager.GetPublic()) {
				t.Fatal("invalid update changed memory or revision")
			}
		})
	}
	manager.filePath = t.TempDir()
	public := publicValue(before)
	public.Filters.IgnoreEcho = true
	if _, err := manager.CompareAndSwap(before.Revision, public); err == nil {
		t.Fatal("expected private write failure")
	}
	if !reflect.DeepEqual(before, manager.GetPublic()) {
		t.Fatal("failed write changed memory or revision")
	}
}

func TestPublicSupportsMultiPlatformContainerIDs(t *testing.T) {
	manager := publicManager(t)
	before := manager.GetPublic()
	public := publicValue(before)
	public.GroupControl.MonitoredGroups = []string{"bilibili:BV1xx411c7mD", "web:v2ex:topic-42"}
	public.GroupControl.ExposedGroups = []string{"bilibili:BV1xx411c7mD"}
	public.GroupControl.GroupAliases = map[string]string{
		"bilibili:BV1xx411c7mD": "Bilibili Demo",
		"web:v2ex:topic-42":     "Web Forum Topic",
	}
	after, err := manager.CompareAndSwap(before.Revision, public)
	if err != nil {
		t.Fatalf("multi-platform public config rejected: %v", err)
	}
	if after.GroupControl.GroupAliases["web:v2ex:topic-42"] != "Web Forum Topic" {
		t.Fatalf("alias lost: %+v", after.GroupControl.GroupAliases)
	}
	for _, invalid := range []string{"bilibili:../bad", "Unknown:123", "bad@@chatroom"} {
		bad := publicValue(after)
		bad.GroupControl.MonitoredGroups = []string{invalid}
		if _, err := manager.CompareAndSwap(after.Revision, bad); !errors.Is(err, ErrInvalidPublic) {
			t.Fatalf("invalid multi-platform entry accepted: %s (%v)", invalid, err)
		}
		badAlias := publicValue(after)
		badAlias.GroupControl.GroupAliases = map[string]string{invalid: "Bad"}
		if _, err := manager.CompareAndSwap(after.Revision, badAlias); !errors.Is(err, ErrInvalidPublic) {
			t.Fatalf("invalid multi-platform alias accepted: %s (%v)", invalid, err)
		}
	}
}
