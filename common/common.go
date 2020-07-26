package common

import (
	"fmt"
	"strings"

	"github.com/beevik/guid"
)

// Record represents a single row which
// type Record map[string]interface{}

type Record struct {
	Data map[string]interface{}
	Id   guid.Guid
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
	for _, field := range fields {
		fieldName := field.Name
		value, ok := r.Data[fieldName]
		if !ok {
			return result, fmt.Errorf("cannot find field %v in target", fieldName)
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
