package storage

import (
	"encoding/json"
)

type KeyValueStoreAccess interface {
	Get(key string) interface{}
	GetBytes(key string) []byte
	GetString(key string) string
	GetStrings(key string) []string
	GetBool(key string) bool
	GetInt(key string) int
	GetInt8(key string) int8
	GetInt16(key string) int16
	GetInt32(key string) int32
	GetInt64(key string) int64
	GetFloat32(key string) float32
	GetFloat64(key string) float64
	Set(key string, val interface{})
	Flush()
	Delete(key string)
}

type Storage struct {
	data map[string]interface{}
}

func New() KeyValueStoreAccess {
	return &Storage{data: make(map[string]interface{})}
}

func NewFromJsonString(jsonString string) KeyValueStoreAccess {
	valueStore := New()

	var jsonKeyValues map[string]interface{}

	if err := json.Unmarshal([]byte(jsonString), &jsonKeyValues); err != nil {
		panic(err)
	}

	for key, value := range jsonKeyValues {
		valueStore.Set(key, value)
	}

	return valueStore
}

func (s *Storage) Get(key string) interface{} {
	return s.data[key]
}

func (s *Storage) GetBytes(key string) []byte {
	return s.data[key].([]byte)
}

func (s *Storage) GetString(key string) string {
	return s.data[key].(string)
}

func (s *Storage) GetStrings(key string) []string {
	return s.data[key].([]string)
}

func (s *Storage) GetBool(key string) bool {
	return s.data[key].(bool)
}

func (s *Storage) GetInt(key string) int {
	return s.data[key].(int)
}

func (s *Storage) GetInt8(key string) int8 {
	return s.data[key].(int8)
}

func (s *Storage) GetInt16(key string) int16 {
	return s.data[key].(int16)
}

func (s *Storage) GetInt32(key string) int32 {
	return s.data[key].(int32)
}

func (s *Storage) GetInt64(key string) int64 {
	return s.data[key].(int64)
}

func (s *Storage) GetFloat32(key string) float32 {
	return s.data[key].(float32)
}

func (s *Storage) GetFloat64(key string) float64 {
	return s.data[key].(float64)
}

func (s *Storage) Set(key string, val interface{}) {
	s.data[key] = val
}

func (s *Storage) Flush() {
	s.data = make(map[string]interface{})
}

func (s *Storage) Delete(key string) {
	delete(s.data, key)
}
