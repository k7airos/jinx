package connector

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	storeSchemaVersion      = 1
	maxCompletedIdempotency = 2000
	idempotencyRetention    = 7 * 24 * time.Hour
	maxCursorSeenPerTarget  = 5000
)

type IdempotencyRecord struct {
	Key        string          `json:"key"`
	Digest     string          `json:"digest"`
	Status     string          `json:"status"` // in_flight | completed
	HTTPStatus int             `json:"http_status,omitempty"`
	Response   *ActionResponse `json:"response,omitempty"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

type idempotencyFile struct {
	Version int                           `json:"version"`
	Records map[string]*IdempotencyRecord `json:"records"`
}

type IdempotencyStore struct {
	mu      sync.Mutex
	path    string
	records map[string]*IdempotencyRecord
	now     func() time.Time
}

func OpenIdempotencyStore(path string) (*IdempotencyStore, error) {
	s := &IdempotencyStore{
		path:    path,
		records: make(map[string]*IdempotencyRecord),
		now:     time.Now,
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, err
	}
	var file idempotencyFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("decode idempotency store: %w", err)
	}
	if file.Version != storeSchemaVersion {
		return nil, fmt.Errorf("unsupported idempotency store version %d", file.Version)
	}
	if file.Records != nil {
		s.records = file.Records
	}
	return s, nil
}

func cloneIdempotencyRecord(rec *IdempotencyRecord) *IdempotencyRecord {
	if rec == nil {
		return nil
	}
	copied := *rec
	if rec.Response != nil {
		resp := *rec.Response
		if rec.Response.Submitted != nil {
			b := *rec.Response.Submitted
			resp.Submitted = &b
		}
		copied.Response = &resp
	}
	return &copied
}

func (s *IdempotencyStore) Claim(key, digest string) (*IdempotencyRecord, string, error) {
	if key == "" || digest == "" {
		return nil, "", fmt.Errorf("idempotency key and digest are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.records[key]; ok {
		if existing.Digest != digest {
			return cloneIdempotencyRecord(existing), "conflict", nil
		}
		if existing.Status == "completed" && existing.Response != nil {
			return cloneIdempotencyRecord(existing), "replay", nil
		}
		return cloneIdempotencyRecord(existing), "in_flight", nil
	}
	rec := &IdempotencyRecord{
		Key:       key,
		Digest:    digest,
		Status:    "in_flight",
		UpdatedAt: s.now().UTC(),
	}
	s.records[key] = rec
	if err := s.saveLocked(); err != nil {
		delete(s.records, key)
		return nil, "", err
	}
	return cloneIdempotencyRecord(rec), "claimed", nil
}

func (s *IdempotencyStore) Complete(key, digest string, httpStatus int, resp ActionResponse) error {
	if key == "" || digest == "" {
		return fmt.Errorf("idempotency key and digest are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.records[key]; ok && existing.Digest != digest {
		return fmt.Errorf("idempotency conflict on completion")
	}
	respCopy := resp
	if resp.Submitted != nil {
		b := *resp.Submitted
		respCopy.Submitted = &b
	}
	now := s.now().UTC()
	s.records[key] = &IdempotencyRecord{
		Key:        key,
		Digest:     digest,
		Status:     "completed",
		HTTPStatus: httpStatus,
		Response:   &respCopy,
		UpdatedAt:  now,
	}
	s.pruneLocked(now)
	return s.saveLocked()
}

func (s *IdempotencyStore) pruneLocked(now time.Time) {
	cutoff := now.Add(-idempotencyRetention)
	var completed []*IdempotencyRecord
	for key, rec := range s.records {
		if rec.Status != "completed" {
			continue
		}
		if rec.UpdatedAt.Before(cutoff) {
			delete(s.records, key)
			continue
		}
		completed = append(completed, rec)
	}
	if len(completed) <= maxCompletedIdempotency {
		return
	}
	sort.Slice(completed, func(i, j int) bool {
		if !completed[i].UpdatedAt.Equal(completed[j].UpdatedAt) {
			return completed[i].UpdatedAt.Before(completed[j].UpdatedAt)
		}
		return completed[i].Key < completed[j].Key
	})
	excess := len(completed) - maxCompletedIdempotency
	for i := 0; i < excess; i++ {
		delete(s.records, completed[i].Key)
	}
}

func (s *IdempotencyStore) saveLocked() error {
	payload := idempotencyFile{
		Version: storeSchemaVersion,
		Records: s.records,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(s.path, data, 0600)
}

type containerCursor struct {
	Seeded bool             `json:"seeded"`
	Seen   map[string]int64 `json:"seen"`
}

type cursorFile struct {
	Version    int                         `json:"version"`
	Containers map[string]*containerCursor `json:"containers"`
}

type CursorStore struct {
	mu         sync.Mutex
	path       string
	containers map[string]*containerCursor
}

func OpenCursorStore(path string) (*CursorStore, error) {
	s := &CursorStore{
		path:       path,
		containers: make(map[string]*containerCursor),
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, err
	}
	var file cursorFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("decode cursor store: %w", err)
	}
	if file.Version != storeSchemaVersion {
		return nil, fmt.Errorf("unsupported cursor store version %d", file.Version)
	}
	if file.Containers != nil {
		s.containers = file.Containers
		for _, c := range s.containers {
			if c.Seen == nil {
				c.Seen = make(map[string]int64)
			}
		}
	}
	return s, nil
}

func (s *CursorStore) ensureContainerLocked(containerID string) *containerCursor {
	c := s.containers[containerID]
	if c == nil {
		c = &containerCursor{Seen: make(map[string]int64)}
		s.containers[containerID] = c
	} else if c.Seen == nil {
		c.Seen = make(map[string]int64)
	}
	return c
}

func (s *CursorStore) HasSeen(containerID, id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.containers[containerID]
	if c == nil || c.Seen == nil {
		return false
	}
	_, ok := c.Seen[id]
	return ok
}

func (s *CursorStore) MarkSeen(containerID, id string, ts int64) error {
	if containerID == "" || id == "" {
		return fmt.Errorf("containerID and id are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.ensureContainerLocked(containerID)
	c.Seen[id] = ts
	if len(c.Seen) > maxCursorSeenPerTarget {
		type entry struct {
			id string
			ts int64
		}
		all := make([]entry, 0, len(c.Seen))
		for k, v := range c.Seen {
			all = append(all, entry{id: k, ts: v})
		}
		sort.Slice(all, func(i, j int) bool {
			if all[i].ts != all[j].ts {
				return all[i].ts < all[j].ts
			}
			return all[i].id < all[j].id
		})
		excess := len(all) - maxCursorSeenPerTarget
		for i := 0; i < excess; i++ {
			delete(c.Seen, all[i].id)
		}
	}
	return s.saveLocked()
}

func (s *CursorStore) IsSeeded(containerID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.containers[containerID]
	return c != nil && c.Seeded
}

func (s *CursorStore) MarkSeeded(containerID string) error {
	if containerID == "" {
		return fmt.Errorf("containerID is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.ensureContainerLocked(containerID)
	if c.Seeded {
		return nil
	}
	c.Seeded = true
	return s.saveLocked()
}

func (s *CursorStore) saveLocked() error {
	payload := cursorFile{
		Version:    storeSchemaVersion,
		Containers: s.containers,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(s.path, data, 0600)
}

func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".connector-*")
	if err != nil {
		return err
	}
	tmpPath := f.Name()
	defer os.Remove(tmpPath)
	if err := f.Chmod(perm); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
