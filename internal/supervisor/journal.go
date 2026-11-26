package supervisor

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

type operationRecord struct {
	Operation Operation  `json:"operation"`
	Plan      ActionPlan `json:"plan"`
	Payload   string     `json:"payload"`
}

type journal struct {
	Schema     int                        `json:"schema"`
	Scope      string                     `json:"scope"`
	Operations map[string]operationRecord `json:"operations"`
}

func (manager *Manager) openJournal() error {
	path := manager.options.DataPath
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return failure("journal_unavailable")
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return failure("journal_unavailable")
	}
	if err := lock.Chmod(0600); err != nil {
		lock.Close()
		return failure("journal_unavailable")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return failure("journal_locked")
	}
	manager.lock = lock
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return manager.persistLocked()
	}
	if err != nil {
		return failure("journal_invalid")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return failure("journal_invalid")
	}
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var stored journal
	if decoder.Decode(&stored) != nil || stored.Schema != 1 || stored.Scope != manager.journal.Scope || stored.Operations == nil {
		return failure("journal_invalid")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return failure("journal_invalid")
	}
	requests := make(map[string]bool)
	for id, record := range stored.Operations {
		operation := record.Operation
		if !safeID(id) || operation.ID != id || !safeID(operation.RequestID) || requests[operation.RequestID] || record.Plan.ID != operation.PlanID || operation.ServiceID != record.Plan.ServiceID || operation.Kind != record.Plan.Kind || !validAction(operation.Kind, operation.ServiceID) || record.Payload == "" {
			return failure("journal_invalid")
		}
		requests[operation.RequestID] = true
		switch operation.State {
		case "succeeded", "failed", "canceled", "unknown":
		case "pending", "draining", "blocked", "executing", "verifying", "canceling":
			operation.State = "unknown"
			operation.Problem = failure("recovered_unknown")
			operation.Revision++
			for index := range operation.Steps {
				if operation.Steps[index].State == "running" {
					operation.Steps[index].State = "unknown"
				}
			}
			record.Operation = operation
			stored.Operations[id] = record
		default:
			return failure("journal_invalid")
		}
	}
	manager.journal = stored
	return manager.persistLocked()
}

func (manager *Manager) persistLocked() error {
	if manager.options.DataPath == "" || manager.poisoned {
		return failure("journal_unavailable")
	}
	data, err := json.Marshal(manager.journal)
	if err != nil {
		manager.poisoned = true
		return failure("journal_unavailable")
	}
	file, err := os.CreateTemp(filepath.Dir(manager.options.DataPath), ".supervisor-*")
	if err != nil {
		manager.poisoned = true
		return failure("journal_unavailable")
	}
	defer os.Remove(file.Name())
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	closeError := file.Close()
	if err == nil {
		err = closeError
	}
	if err == nil {
		err = os.Rename(file.Name(), manager.options.DataPath)
	}
	if err == nil {
		var directory *os.File
		directory, err = os.Open(filepath.Dir(manager.options.DataPath))
		if err == nil {
			err = directory.Sync()
			directory.Close()
		}
	}
	if err != nil {
		manager.poisoned = true
		return failure("journal_unavailable")
	}
	return nil
}

func clone[T any](value T) T {
	data, _ := json.Marshal(value)
	var result T
	json.Unmarshal(data, &result)
	return result
}
