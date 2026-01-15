package common

import (
	"fmt"
	"os"
	"strconv"
)

func GetEnv[T any](key string, defaultValue T) (T, error) {
	v, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue, nil
	}

	var err error
	var parsed any

	switch any(defaultValue).(type) {
	case string:
		return any(v).(T), nil
	case uint16:
		var p uint64
		p, err = strconv.ParseUint(v, 10, 16)
		parsed = uint16(p)
	case int:
		parsed, err = strconv.Atoi(v)
	default:
		return defaultValue, fmt.Errorf("unsupported type for env var %s: %T", key, defaultValue)
	}

	if err != nil {
		return defaultValue, fmt.Errorf("failed to parse env %s as %T: %w", key, defaultValue, err)
	}
	return parsed.(T), err
}
