package common

import (
	"encoding/json"
)

// PrettyPrint returns a jsoned indented string
func PrettyPrint(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
