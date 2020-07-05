package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"gopkg.in/yaml.v2"
)

// Field represents the attributes of a field of the Metadata
type Field struct {
	Name         string `yaml:"name"`
	ExpectedType string `yaml:"type"`
}

// Metadata represents the definition of a single row of data
type Metadata struct {
	Fields []Field `yaml:"fields"`
}

// ParseMetadata converts a string to a Metadata instance
func ParseMetadata(data string) (*Metadata, error) {
	var result = new(Metadata)
	err := yaml.UnmarshalStrict([]byte(data), &result)
	return result, err
}

// FieldCount returns the amount of fields of a metadata instance
func (m *Metadata) FieldCount() int {
	return len(m.Fields)
}

// Validate checks a fields data and returns error if it cannot be converted to the expected type
func (f *Field) Validate(data interface{}) (interface{}, error) {
	var isOk = true
	var result interface{} = nil
	var err error

	switch f.ExpectedType {
	case "int":
		switch data.(type) {
		case string:
			result, err = strconv.Atoi(fmt.Sprintf("%v", data))
			if err != nil {
				isOk = false
			}
		case int:
			result = data.(int)
			isOk = true
		default:
			isOk = false
		}
	case "string":
		result = data.(string)
		isOk = true
	case "bool":
		switch data.(type) {
		case string:
			result, err = strconv.ParseBool(fmt.Sprintf("%v", data))
			if err != nil {
				isOk = false
			}
		case bool:
			result = data.(bool)
			isOk = true
		case int:
			result = data.(int)
			if !(result == 0 || result == 1) {
				isOk = false
			}
		}
	default:
		return nil, fmt.Errorf("unexpected type %v of field %v", f.ExpectedType, f.Name)
	}

	if !isOk {
		return nil, fmt.Errorf("cannot convert field %v with value %v to %v", f.Name, data, f.ExpectedType)
	}
	return result, nil
}

// Validate deserializes json data into an array and checks every field against the attributes of the metadata instance
func (m *Metadata) Validate(jsonData []byte) ([]interface{}, error) {
	errString := ""
	parsed := make([]interface{}, m.FieldCount())

	err := json.Unmarshal(jsonData, &parsed)
	if err != nil {
		return nil, err
	}
	if len(parsed) < m.FieldCount() {
		return nil, fmt.Errorf("row length (%v) is less than metadata fields (%v)", len(parsed), m.FieldCount())
	}
	if len(parsed) > m.FieldCount() {
		return nil, fmt.Errorf("row length (%v) is greater than metadata fields (%v)", len(parsed), m.FieldCount())
	}

	result := make([]interface{}, m.FieldCount())

	for index, field := range m.Fields {
		converted, err := field.Validate(parsed[index])
		if err != nil {
			errString = fmt.Sprintf("%v\n%v", errString, err)
		} else {
			result[index] = converted
		}
	}

	if errString != "" {
		return nil, errors.New(errString)
	}

	return result, nil
}
