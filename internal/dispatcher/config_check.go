package dispatcher

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
)

func ValidateConfigFile(configPath, localPolicyPath string) error {
	if localPolicyPath == "" {
		localPolicyPath = os.Getenv("JINX_LOCAL_POLICY")
	}
	policy, err := loadLocalPolicy(localPolicyPath)
	if errors.Is(err, errPolicyFileMode) {
		return errors.New("local policy validation failed: " + errPolicyFileMode.Error())
	}
	if err != nil {
		return errors.New("local policy validation failed")
	}
	file, err := os.Open(configPath)
	if err != nil {
		return errors.New("dispatcher config unreadable or too large")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 4<<20 {
		return errors.New("dispatcher config unreadable or too large")
	}
	data, err := io.ReadAll(io.LimitReader(file, (4<<20)+1))
	if err != nil || len(data) > 4<<20 {
		return errors.New("dispatcher config unreadable or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if decoder.Decode(new(json.RawMessage)) != nil || decoder.Decode(new(any)) != io.EOF {
		return errors.New("invalid dispatcher configuration")
	}
	if _, err := decodeConfigWithPolicy(data, policy); err != nil {
		return errors.New("invalid dispatcher configuration")
	}
	return nil
}
