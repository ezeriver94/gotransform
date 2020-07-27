package common

import (
	"fmt"
	"strconv"
)

// ValidateString checks if one value can be converted to string
func (f *Field) ValidateString(data interface{}) (string, error) {
	return data.(string), nil

}

// ValidateBool checks if one value can be converted to bool
func (f *Field) ValidateBool(data interface{}) (bool, error) {
	switch data.(type) {
	case string:
		result, err := strconv.ParseBool(fmt.Sprintf("%v", data))
		if err != nil {
			return false, err
		}
		return result, nil
	case bool:
		return data.(bool), nil
	case int:
		if data.(int) == 0 {
			return false, nil
		} else if data.(int) == 1 {
			return true, nil
		}
		return false, fmt.Errorf("cannot convert int value %v to bool; allowed int values are 0 or 1", data)
	case float32:
		if data.(float32) == 0 {
			return false, nil
		} else if data.(float32) == 1 {
			return true, nil
		}
		return false, fmt.Errorf("cannot convert float32 value %v to bool; allowed int values are 0 or 1", data)
	case float64:
		if data.(float64) == 0 {
			return false, nil
		} else if data.(float64) == 1 {
			return true, nil
		}
		return false, fmt.Errorf("cannot convert float32 value %v to bool; allowed int values are 0 or 1", data)
	default:
		return false, fmt.Errorf("cannot convert from type %T to bool", data)
	}
}

// Validate validates the data acording to the field spec
func (f *Field) Validate(data interface{}) (interface{}, error) {
	switch f.ExpectedType {
	case "int":
		return f.ValidateInt(data)
	case "string":
		return f.ValidateString(data)
	case "bool":
		return f.ValidateBool(data)
	default:
		return nil, fmt.Errorf("unknown type %v in field %v", f.ExpectedType, f.Name)
	}
}

// ValidateInt checks if one value can be converted to int
func (f *Field) ValidateInt(data interface{}) (int, error) {
	switch data.(type) {
	case string:
		result, err := strconv.Atoi(fmt.Sprintf("%v", data))
		if err != nil {
			return 0, err
		}
		return result, nil
	case int:
		return data.(int), nil
	default:
		return 0, fmt.Errorf("cannot convert from type %T to int", data)
	}
}

// FieldCount returns the amount of fields defined within a datasource
func (ds *DataEndpoint) FieldCount() int {
	return len(ds.Fields)
}

// Validate deserializes json data into an array and checks every field against the attributes of the metadata instance
func (ds *DataEndpoint) Validate(record *Record) error {
	errString := ""
	fieldCount := ds.FieldCount()
	if record.Length() < ds.FieldCount() {
		return fmt.Errorf(record.Log("row length (%v) is less than metadata fields (%v)", record.Length(), fieldCount))
	}
	if record.Length() > ds.FieldCount() {
		return fmt.Errorf(record.Log("row length (%v) is greater than metadata fields (%v)", record.Length(), fieldCount))
	}
	record.StartUnraw()
	for index, field := range ds.Fields {
		data, err := record.Get(index)
		if err != nil {
			errString = fmt.Sprintf("%v\n%v", errString, err)
			continue
		}
		converted, err := field.Validate(data)
		if err != nil {
			errString = fmt.Sprintf("%v\n%v", errString, err)
			continue
		}
		err = record.Set(field.Name, converted)
		if err != nil {
			errString = fmt.Sprintf("%v\n%v", errString, err)
			continue
		}
	}
	record.EndUnraw()
	if errString != "" {
		return fmt.Errorf(record.Log("error validating record: %v", errString))
	}

	return nil
}
