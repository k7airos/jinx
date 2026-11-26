package observability

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"jinx/internal/contracts"
)

var ErrWALCorrupt = errors.New("WAL corrupt or incomplete; preserve files and restore verified data before reopening")

const (
	frameMagic  = "JINXWAL1"
	commitMagic = "COMMIT01"
	frameHeader = 48
	frameFooter = 16
)

type walFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

type walHeader struct {
	Schema   int    `json:"schema"`
	StreamID string `json:"streamId"`
	BaseSeq  uint64 `json:"baseSeq"`
}

type walRecord struct {
	Seq      uint64           `json:"seq"`
	Run      *contracts.Run   `json:"run,omitempty"`
	Event    *contracts.Event `json:"event,omitempty"`
	FloorSeq *uint64          `json:"floorSeq,omitempty"`
}

func decodeJSON(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return fmt.Errorf("trailing data")
	}
	return nil
}

func encodeFrame(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, frameHeader+len(payload)+frameFooter)
	copy(frame, frameMagic)
	binary.BigEndian.PutUint64(frame[8:16], uint64(len(payload)))
	checksum := sha256.Sum256(payload)
	copy(frame[16:frameHeader], checksum[:])
	copy(frame[frameHeader:], payload)
	copy(frame[frameHeader+len(payload):], commitMagic)
	binary.BigEndian.PutUint64(frame[len(frame)-8:], uint64(len(payload)))
	return frame, nil
}

func readFrame(file *os.File, offset, size int64, value any) (int64, error) {
	corrupt := func(reason any) (int64, error) {
		return offset, fmt.Errorf("%w at byte %d: %v", ErrWALCorrupt, offset, reason)
	}
	if size-offset < frameHeader+frameFooter {
		return corrupt("incomplete frame")
	}
	var header [frameHeader]byte
	if _, err := file.ReadAt(header[:], offset); err != nil {
		return corrupt(err)
	}
	length := binary.BigEndian.Uint64(header[8:16])
	if string(header[:8]) != frameMagic || length > uint64(size-offset-frameHeader-frameFooter) {
		return corrupt("invalid header or truncated payload")
	}
	payload := make([]byte, int(length))
	if _, err := file.ReadAt(payload, offset+frameHeader); err != nil {
		return corrupt(err)
	}
	var footer [frameFooter]byte
	if _, err := file.ReadAt(footer[:], offset+frameHeader+int64(length)); err != nil {
		return corrupt(err)
	}
	checksum := sha256.Sum256(payload)
	if !bytes.Equal(checksum[:], header[16:]) || string(footer[:8]) != commitMagic || binary.BigEndian.Uint64(footer[8:]) != length {
		return corrupt("checksum or commit marker mismatch")
	}
	if err := decodeJSON(payload, value); err != nil {
		return corrupt(err)
	}
	return offset + frameHeader + int64(length) + frameFooter, nil
}

func (store *Store) openWAL(allowCreate bool) error {
	file, err := os.OpenFile(store.path+".wal", os.O_RDWR|os.O_APPEND, 0600)
	if errors.Is(err, os.ErrNotExist) && allowCreate && store.state.WALSeq == 0 {
		file, err = os.OpenFile(store.path+".wal", os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
		store.wal = file
		frame, err := encodeFrame(walHeader{Schema: 1, StreamID: store.state.StreamID})
		if err != nil {
			return err
		}
		if err := store.writeWAL(frame); err != nil {
			return err
		}
		if err := syncDirectory(store.path); err != nil {
			return store.persistenceUnknown(err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("open required WAL: %w", err)
	}
	store.wal = file
	info, err := file.Stat()
	if err != nil {
		return err
	}
	var header walHeader
	offset, err := readFrame(file, 0, info.Size(), &header)
	if err != nil {
		return err
	}
	checkpointSeq := store.state.WALSeq
	if header.Schema != 1 || header.StreamID != store.state.StreamID || header.BaseSeq > checkpointSeq {
		return fmt.Errorf("%w: WAL header disagrees with snapshot", ErrWALCorrupt)
	}
	sequence := header.BaseSeq
	for offset < info.Size() {
		var record walRecord
		next, err := readFrame(file, offset, info.Size(), &record)
		if err != nil {
			return err
		}
		if record.Seq != sequence+1 || sequence == ^uint64(0) {
			return fmt.Errorf("%w at byte %d: transaction sequence gap", ErrWALCorrupt, offset)
		}
		if err := store.validateRecord(record, record.Seq > checkpointSeq); err != nil {
			return fmt.Errorf("%w at byte %d: %v", ErrWALCorrupt, offset, err)
		}
		if record.Seq > checkpointSeq {
			store.applyRecord(record)
		}
		sequence, offset = record.Seq, next
	}
	if sequence < checkpointSeq {
		return fmt.Errorf("%w: WAL ends before checkpoint watermark", ErrWALCorrupt)
	}
	if err := file.Sync(); err != nil {
		return store.persistenceUnknown(err)
	}
	if err := syncDirectory(store.path); err != nil {
		return store.persistenceUnknown(err)
	}
	return nil
}

func (store *Store) validateRecord(record walRecord, apply bool) error {
	if record.FloorSeq != nil {
		if record.Run != nil || record.Event != nil || *record.FloorSeq == 0 || *record.FloorSeq > store.state.LastSeq || (apply && *record.FloorSeq <= store.state.FloorSeq) || (!apply && *record.FloorSeq > store.state.FloorSeq) {
			return fmt.Errorf("invalid prune record")
		}
		return nil
	}
	if record.Run == nil || record.Event == nil {
		return fmt.Errorf("missing run or event")
	}
	run, event := *record.Run, *record.Event
	if err := contracts.ValidateRun(run); err != nil {
		return err
	}
	if err := validateEventCorrelation(run, event); err != nil {
		return err
	}
	if run.Revision == "" || run.EntitySeq == 0 || event.StreamID != store.state.StreamID || event.Seq != run.EntitySeq || event.Cursor != cursor(store.state.StreamID, event.Seq) || event.Entity != "run" || event.EntityID != run.ID || event.Revision != run.Revision || event.Kind == "" || !event.OccurredAt.Equal(run.UpdatedAt) || event.Phase != run.Phase || event.Outcome != run.Outcome || event.Delivery != run.Delivery || event.ReasonCode != run.ReasonCode {
		return fmt.Errorf("run and event disagree")
	}
	if !apply {
		history := store.state.History[run.ID]
		index := sort.Search(len(history), func(index int) bool { return history[index].Seq >= event.Seq })
		if index == len(history) || !reflect.DeepEqual(history[index], event) {
			return fmt.Errorf("checkpoint and WAL history disagree")
		}
	}
	if apply {
		if store.state.LastSeq == ^uint64(0) || event.Seq != store.state.LastSeq+1 {
			return fmt.Errorf("event sequence gap")
		}
		if previous, exists := store.state.Runs[run.ID]; exists {
			if err := contracts.ValidateTransition(previous, run); err != nil {
				return err
			}
		} else if run.Phase != contracts.PhaseAccepted || run.Outcome != contracts.OutcomePending || run.Delivery != contracts.DeliveryNotStarted || len(run.Attempts) != 0 {
			return fmt.Errorf("new run is not accepted")
		}
	}
	return nil
}

func (store *Store) applyRecord(record walRecord) {
	store.state.WALSeq = record.Seq
	if record.FloorSeq != nil {
		count := *record.FloorSeq - store.state.FloorSeq
		clear(store.state.Events[:count])
		store.state.Events = store.state.Events[count:]
		store.state.FloorSeq = *record.FloorSeq
		return
	}
	run, event := *record.Run, *record.Event
	store.state.LastSeq = event.Seq
	store.state.Runs[run.ID] = run
	store.state.Events = append(store.state.Events, event)
	store.state.History[run.ID] = append(store.state.History[run.ID], event)
}

func (store *Store) appendWAL(record walRecord) error {
	if store.state.WALSeq == ^uint64(0) || (record.Run != nil && store.state.LastSeq == ^uint64(0)) {
		return fmt.Errorf("store sequence exhausted")
	}
	frame, err := encodeFrame(record)
	if err != nil {
		return err
	}
	return store.writeWAL(frame)
}

func (store *Store) writeWAL(frame []byte) error {
	written, err := store.wal.Write(frame)
	if err == nil && written != len(frame) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return store.persistenceUnknown(err)
	}
	if err := store.wal.Sync(); err != nil {
		return store.persistenceUnknown(err)
	}
	return nil
}

func (store *Store) persistenceUnknown(err error) error {
	if !store.uncertain {
		store.uncertain = true
		close(store.failed)
	}
	return fmt.Errorf("%w: %v", ErrPersistenceUnknown, err)
}

func syncDirectory(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (store *Store) writeSnapshot() error {
	data, err := json.Marshal(store.state)
	if err != nil {
		return err
	}
	return store.replaceFile(store.path, data)
}

func (store *Store) replaceFile(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".run-store-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	written, err := file.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	if err := syncDirectory(path); err != nil {
		return store.persistenceUnknown(err)
	}
	return nil
}

func (store *Store) Checkpoint() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready(); err != nil {
		return err
	}
	if err := store.writeSnapshot(); err != nil {
		return err
	}
	frame, err := encodeFrame(walHeader{Schema: 1, StreamID: store.state.StreamID, BaseSeq: store.state.WALSeq})
	if err != nil {
		return err
	}
	if err := store.replaceFile(store.path+".wal", frame); err != nil {
		return store.persistenceUnknown(fmt.Errorf("WAL rotation: %w", err))
	}
	previous := store.wal
	store.wal, err = os.OpenFile(store.path+".wal", os.O_RDWR|os.O_APPEND, 0600)
	closeErr := previous.Close()
	if err != nil || closeErr != nil {
		return store.persistenceUnknown(errors.Join(err, closeErr))
	}
	return nil
}
