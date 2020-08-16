package common

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/beevik/guid"
)

// Record represents a single row, with an ID to track it through every phase
type Record struct {
	data     map[string]interface{}
	rawData  []interface{}
	ID       *guid.Guid
	Empty    bool
	raw      bool
	unrawing bool
}

// ErrMissingItemOnRecord indicates that a key was not found on record values
var ErrMissingItemOnRecord = errors.New("missing item on record")

// NewRecord creates an empty record with a new GUID
func NewRecord(raw bool) Record {
	return Record{
		data:    nil,
		rawData: nil,
		ID:      guid.New(),
		raw:     raw,
		Empty:   true,
	}
}

// StartUnraw sets raw property of record to false and enables unrawing property
func (r *Record) StartUnraw() {
	r.raw = false
	r.unrawing = true
	r.data = make(map[string]interface{})
}

// EndUnraw removes rawData from record and disable unrawing property
func (r *Record) EndUnraw() {
	r.unrawing = false
	r.rawData = nil
}

// MarshalJSON serializes a record to json
func (r Record) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	buffer.WriteString("\"guid\" :")
	guidData, err := json.Marshal(r.ID.String())
	if err != nil {
		return nil, err
	}
	buffer.Write(guidData)
	buffer.WriteString(",")
	rawData, err := json.Marshal(r.raw)
	if err != nil {
		return nil, err
	}
	buffer.WriteString("\"raw\":")
	buffer.Write(rawData)
	buffer.WriteString(",")
	if r.raw {
		info, err := json.Marshal(r.rawData)
		if err != nil {
			return nil, err
		}
		buffer.WriteString("\"data\":")
		buffer.WriteString(string(info))
	} else {
		info, err := json.Marshal(r.data)
		if err != nil {
			return nil, err
		}
		buffer.WriteString("\"data\":")
		buffer.Write(info)
	}

	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

// UnmarshalJSON serializes a record to json
func (r *Record) UnmarshalJSON(b []byte) error {
	type PlainData struct {
		Guid string `json:"guid"`
		Raw  bool   `json:"raw"`
	}
	var plainData PlainData
	err := json.Unmarshal(b, &plainData)
	if err != nil {
		return err
	}
	r.raw = plainData.Raw
	r.ID, err = guid.ParseString(plainData.Guid)
	if err != nil {
		return err
	}
	if r.raw {
		type RawData struct {
			Data []interface{} `json:"data"`
		}

		var rawData RawData
		err := json.Unmarshal(b, &rawData)
		if err != nil {
			return err
		}
		r.rawData = rawData.Data
		r.Empty = len(r.rawData) == 0
	} else {
		type Data struct {
			Data map[string]interface{} `json:"data"`
		}

		var data Data
		err := json.Unmarshal(b, &data)
		if err != nil {
			return err
		}
		r.data = data.Data
		r.Empty = len(r.data) == 0
	}

	return nil
}

// Set adds a new entry in the record
func (r *Record) Set(key interface{}, value interface{}) error {
	if r.Empty {
		if r.raw {
			r.rawData = make([]interface{}, 0)
		} else {
			r.data = make(map[string]interface{})
		}
		r.Empty = false
	}
	if r.raw {
		switch key.(type) {
		case int:
			if len(r.rawData) > key.(int) {
				r.rawData[key.(int)] = value
			} else {
				r.rawData = append(r.rawData, value)
			}
		default:
			r.rawData = append(r.rawData, value)
		}
	} else {
		switch key.(type) {
		case string:
			r.data[key.(string)] = value
		default:
			return fmt.Errorf("expected string identifier to get Data of record, received %T - %v", key, key)
		}
	}
	return nil
}

// Get returns a single value of the record; if the record is raw, identifier is expected to be the index of the slice, otherwise is the key of the Data map
func (r *Record) Get(identifier interface{}) (interface{}, error) {
	if r.Empty {
		return nil, ErrMissingItemOnRecord
	}

	if r.raw || r.unrawing {
		switch identifier.(type) {
		case int:
			if len(r.rawData) < identifier.(int)+1 {
				return nil, ErrMissingItemOnRecord
			}
			return r.rawData[identifier.(int)], nil
		default:
			return nil, fmt.Errorf("expected int identifier to get RawData of record, received %v", identifier)
		}
	} else {
		switch identifier.(type) {
		case string:
			if found, ok := r.data[identifier.(string)]; ok {
				return found, nil
			}
			return nil, ErrMissingItemOnRecord
		default:
			return nil, fmt.Errorf("expected string identifier to get Data of record, received %v", identifier)
		}
	}
}

// IsSet returns a boolean indicating if certain value of a record exists or not
func (r *Record) IsSet(identifier interface{}) (bool, error) {
	if r.Empty {
		return false, nil
	}
	if r.raw {
		switch identifier.(type) {
		case int:
			return len(r.rawData) < identifier.(int)+1, nil
		default:
			return false, fmt.Errorf("expected int identifier to get RawData of record, received %v", identifier)
		}
	} else {
		switch identifier.(type) {
		case string:
			if _, ok := r.data[identifier.(string)]; ok {
				return true, nil
			}
			return false, nil
		default:
			return false, fmt.Errorf("expected string identifier to get Data of record, received %v", identifier)
		}
	}
}

// Length returns the length of a record
func (r *Record) Length() int {
	if r.Empty {
		return 0
	}
	if r.raw {
		return len(r.rawData)
	}
	return len(r.data)
}

// PopulateFromJSON receives a json value and populates a record from its deserialization
func (r *Record) PopulateFromJSON(data string) error {
	if r.raw {
		finalData := make([]interface{}, 0)
		err := json.Unmarshal([]byte(data), &finalData)
		if err != nil {
			return err
		}
		for i, item := range finalData {
			err := r.Set(i, item)
			if err != nil {
				return err
			}
		}
	} else {
		finalData := make(map[string]interface{})
		err := json.Unmarshal([]byte(data), &finalData)
		if err != nil {
			return err
		}
		for key, value := range finalData {
			err := r.Set(key, value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Log returns a string with a message concatenated to the GUID of the record
func (r *Record) Log(message string, args ...interface{}) string {
	return fmt.Sprintf("GUID: %v | %v", r.ID.String(), fmt.Sprintf(message, args...))
}

// TryGet receives two arguments to use as a key for Get method, and uses the correct one according to the raw field
func (r *Record) TryGet(key string, index int) (interface{}, error) {
	if r.raw || r.unrawing {
		return r.Get(index)
	}
	return r.Get(key)
}

// TrySet sets the value on a record using raw information for de indexer
func (r *Record) TrySet(key string, index int, value interface{}) error {
	if r.raw {
		return r.Set(index, value)
	}
	return r.Set(key, value)
}

func FieldToString(data interface{}) string {
	switch data.(type) {
	case bool:
		if data.(bool) == true {
			return "1"
		}
		return "0"
	default:
		if data == nil {
			return ""
		}
		return fmt.Sprint(data)
	}
}
func FieldToRuneArray(data interface{}) []rune {
	switch data.(type) {
	case bool:
		if data.(bool) == true {
			return []rune("1")
		}
		return []rune("0")
	default:
		if data == nil {
			return []rune("")
		}
		return []rune(fmt.Sprint(data))
	}
}

func (r *Record) ToString(fields Fields) (string, error) {
	result := ""
	for index, field := range fields {
		fieldName := field.Name

		value, err := r.TryGet(fieldName, index)
		if err == ErrMissingItemOnRecord {
			return result, fmt.Errorf("cannot find field %v in target", fieldName)
		}
		if err != nil {
			return result, fmt.Errorf("error trying to find value of field %v on record %v", fieldName, r)
		}
		var fieldValue string
		runeArrayValue := FieldToRuneArray(value)
		stringValue := string(runeArrayValue)
		if field.FixedLength > 0 {
			if len(runeArrayValue) > field.FixedLength {
				return result, fmt.Errorf("field %v has fixed length of %v and current value %v has longer length (%v)", field.Name, field.FixedLength, stringValue, len(runeArrayValue))
			}
			if len(runeArrayValue) == field.FixedLength {
				fieldValue = stringValue
			} else {
				if len(field.Padding.Char) != 1 {
					return result, fmt.Errorf("field %v has fixed length but padding character has not length of 1", field.Name)
				}
				if field.Padding.Mode == FieldPaddingLeft {
					fieldValue = strings.Repeat(field.Padding.Char, field.FixedLength-len(runeArrayValue)) + stringValue
				} else if field.Padding.Mode == FieldPaddingRight {
					fieldValue = stringValue + strings.Repeat(field.Padding.Char, field.FixedLength-len(runeArrayValue))
				}
			}
		} else {
			if len(field.EndCharacter) != 1 {
				return result, fmt.Errorf("field %v has no fixed length and end character has not length of 1", field.Name)
			}
			if len(runeArrayValue) > field.MaxLength {
				return result, fmt.Errorf("field %v has max length of %v and value %v is longer than that (%v)", field.Name, field.MaxLength, stringValue, len(runeArrayValue))
			}
			fieldValue = stringValue
			if len(runeArrayValue) < field.MaxLength {
				fieldValue += field.EndCharacter
			}
		}

		result += fieldValue
	}
	return result, nil
}
