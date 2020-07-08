package common

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ezeriver94/gotransform/dataprovider"
)

// Transformed is the result of a transformation applied
type Transformed map[string]interface{}

// Transform applies transformation rules to input fields of a datasource
func (t *DataTransformation) Transform(dataSourceFields map[string]interface{}) (Transformed, error) {
	result := make(Transformed)

	for key, sel := range t.Select {
		dataSource, field, err := sel.Parse()
		if err != nil {
			return nil, err
		}
		var value interface{}

		if dataSource == t.From {
			var ok bool
			value, ok = dataSourceFields[field]
			if !ok {
				return nil, fmt.Errorf("cannot find expected field %v in primary datasource values %v", field, dataSourceFields)
			}
		} else {
			var err error
			value, err = dataprovider.Get()
			if err != nil {
				return nil, err
			}
		}
		result[key] = value
	}

	return result, nil
}

// Parse returns the components of a select clause, splitting it by '.'
func (sc SelectClause) Parse() (string, string, error) {
	result := strings.Split(string(sc), ".")
	if len(result) == 2 {
		return result[0], result[1], nil
	}
	return "", "", fmt.Errorf("cannot split select clause %v", sc)
}

// DeserializeTransformed deserializes a message to a Transformed instance
func DeserializeTransformed(data []byte) (Transformed, error) {
	var result Transformed
	err := json.Unmarshal(data, &result)
	return result, err
}

// Serialize converts a transformed result to a json string
func (t *Transformed) Serialize() ([]byte, error) {
	return json.Marshal(t)
}
