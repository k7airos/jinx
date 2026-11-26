package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestUnknownExtensionsRetainedAndInvalidateCAS(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	raw := `{"local_db":{"enabled":true,"opaque":[1,2]},"future":{"token":"synthetic"}}`
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(path)
	if err != nil {
		t.Fatal(err)
	}
	before := manager.GetExtension("local_db")
	var expected interface{}
	_ = json.Unmarshal(before, &expected)
	for _, update := range []func() error{
		func() error { return manager.Update(manager.Get()) },
		func() error { _, err := manager.UpdatePublic(publicValue(manager.GetPublic())); return err },
		func() error {
			current := manager.GetPublic()
			_, err := manager.CompareAndSwap(current.Revision, publicValue(current))
			return err
		},
	} {
		if err := update(); err != nil {
			t.Fatal(err)
		}
		reloaded, err := NewManager(path)
		var actual interface{}
		if err == nil {
			err = json.Unmarshal(reloaded.GetExtension("local_db"), &actual)
		}
		if err != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatal("extension lost", err)
		}
	}
	snapshot := manager.GetPublic()
	value := manager.GetExtension("future")
	value[0] = '['
	if !json.Valid(manager.GetExtension("future")) {
		t.Fatal("GetExtension shares bytes")
	}
	value = json.RawMessage(`{"token":"changed"}`)
	if err := manager.UpdateExtension("future", value); err != nil {
		t.Fatal(err)
	}
	value[0] = '['
	if !json.Valid(manager.GetExtension("future")) {
		t.Fatal("UpdateExtension shares bytes")
	}
	if _, err := manager.CompareAndSwap(snapshot.Revision, publicValue(snapshot)); err != ErrRevisionConflict {
		t.Fatal("extension update did not invalidate CAS", err)
	}
	encoded, _ := json.Marshal(manager.Get())
	if strings.Contains(string(encoded), "local_db") || strings.Contains(string(encoded), "token") {
		t.Fatal("public config leaked extensions")
	}
}
func TestConfigCopiesAndFailedSave(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := mgr.Get()
	cfg.GroupControl.GroupAliases["x"] = "changed"
	if mgr.Get().GroupControl.GroupAliases["x"] != "" {
		t.Fatal("Get shared its map")
	}
	mgr.filePath = dir
	cfg.Filters.IgnoreEcho = true
	if mgr.Update(cfg) == nil {
		t.Fatal("expected write failure")
	}
	if mgr.Get().Filters.IgnoreEcho {
		t.Fatal("failed save changed active config")
	}
}

func TestDefaultLoopbackAndUpstreamValidation(t *testing.T) {
	def := DefaultConfig()
	if def.Server.ListenAddr != "127.0.0.1:8080" || def.Server.EnableCORS {
		t.Fatalf("unsafe defaults: %+v", def.Server)
	}
	mgr := publicManager(t)
	bad := mgr.Get()
	bad.Upstream.Enabled = true
	bad.Upstream.WSURL = "ws://evil.example:5032"
	if err := mgr.Update(bad); err == nil {
		t.Fatal("expected non-loopback enabled upstream to be rejected")
	}
	bad.Upstream.WSURL = "ws://127.0.0.1:5032/ws?token=leak"
	if err := mgr.Update(bad); err == nil {
		t.Fatal("expected query in upstream URL to be rejected")
	}
	bad.Upstream.WSURL = "ws://127.0.0.1:5032"
	if err := mgr.Update(bad); err != nil {
		t.Fatalf("expected loopback upstream to pass, got %v", err)
	}
}
