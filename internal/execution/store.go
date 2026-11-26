package execution

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"
)

const maxConfigBytes = 4 << 20

type receipt struct {
	Digest   string `json:"digest"`
	Revision string `json:"revision"`
}

type diskState struct {
	Snapshot
	Receipts map[string]receipt `json:"receipts"`
}

func digest(value any) string {
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func newRevision() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func strictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 64 {
			return failure("validation", "json_depth")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if delimiter, ok := token.(json.Delim); ok {
			keys := map[string]bool{}
			for decoder.More() {
				if delimiter == '{' {
					key, err := decoder.Token()
					if err != nil {
						return err
					}
					name, ok := key.(string)
					if !ok || keys[name] {
						return failure("validation", "duplicate_json_key")
					}
					keys[name] = true
				}
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		}
		return nil
	}
	if err := walk(0); err != nil {
		return failure("validation", "json")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return failure("validation", "trailing_json")
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	var shape any
	if err := json.Unmarshal(data, &shape); err != nil || !completeShape(shape, reflect.TypeOf(target).Elem()) {
		return failure("validation", "required_fields_or_null")
	}
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return failure("validation", "json_schema")
	}
	return nil
}

func completeShape(value any, target reflect.Type) bool {
	if value == nil {
		return false
	}
	switch target.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}
		fields := map[string]reflect.Type{}
		var collect func(reflect.Type)
		collect = func(current reflect.Type) {
			for index := 0; index < current.NumField(); index++ {
				field := current.Field(index)
				if field.Anonymous {
					collect(field.Type)
					continue
				}
				name := strings.Split(field.Tag.Get("json"), ",")[0]
				fields[name] = field.Type
			}
		}
		collect(target)
		if len(fields) != len(object) {
			return false
		}
		for name, fieldType := range fields {
			if !completeShape(object[name], fieldType) {
				return false
			}
		}
	case reflect.Map:
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for _, child := range object {
			if !completeShape(child, target.Elem()) {
				return false
			}
		}
	case reflect.Slice:
		array, ok := value.([]any)
		if !ok {
			return false
		}
		for _, child := range array {
			if !completeShape(child, target.Elem()) {
				return false
			}
		}
	}
	return true
}

func DecodeConfig(reader io.Reader) (Config, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxConfigBytes+1))
	if err != nil {
		return Config{}, err
	}
	if len(data) > maxConfigBytes {
		return Config{}, failure("too_large", "config")
	}
	var config Config
	err = strictJSON(data, &config)
	return config, err
}

func (engine *Engine) locked(ctx context.Context, action func() error) error {
	lock, err := os.OpenFile(engine.path+".lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	for {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if err := ctx.Err(); err != nil {
		return err
	}
	return action()
}

func (engine *Engine) read() (diskState, error) {
	file, err := os.Open(engine.path)
	if err != nil {
		return diskState{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil || len(data) > maxConfigBytes {
		return diskState{}, failure("unavailable", "config_read")
	}
	var state diskState
	if err := strictJSON(data, &state); err != nil {
		return state, err
	}
	if state.Revision == "" || state.Receipts == nil {
		return state, failure("validation", "store_schema")
	}
	if err := engine.validate(state.Config); err != nil {
		return state, err
	}
	return state, nil
}

func (engine *Engine) write(state diskState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if len(data) > maxConfigBytes {
		return failure("too_large", "store")
	}
	directory := filepath.Dir(engine.path)
	file, err := os.CreateTemp(directory, ".execution-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(file.Name(), engine.path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return failure("persistence_unknown", "directory_open")
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return failure("persistence_unknown", "directory_fsync")
	}
	return nil
}
