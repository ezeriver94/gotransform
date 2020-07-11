package dataprovider

import (
	"fmt"

	"github.com/ezeriver94/gotransform/common"
)

// Request contains the information needed for the dataprovider to fetch data
type Request struct {
	ObjectID string
	Filters  map[common.Field]interface{}
}

// Record represents a single row which
type Record map[string]interface{}

// ConnectionMode indicates the type of connection
type ConnectionMode int

const (
	ConnectionModeRead ConnectionMode = iota
	ConenctionModeWrite
)

// DataProvider is a class that can perform actions against a datasource (fetching and saving data)
type DataProvider interface {
	Connect(conectionString, objectID string, fields []common.Field, connectionMode ConnectionMode) error

	Fetch(r Request) (Record, error)

	Stream(r Request, buffer chan<- []interface{}) error
	Save(buffer <-chan Record) error

	Close() error
}

// NewRequest creates a request
func NewRequest(objectID string, filters map[common.Field]interface{}) Request {
	return Request{
		ObjectID: objectID,
		Filters:  filters,
	}
}

// NewDataProvider creates an instance of a dataprovider according to the driver passed as argument
func NewDataProvider(driver string) (DataProvider, error) {
	var result DataProvider = nil
	switch driver {
	case "plaintext":
		result = &PlainTextDataProvider{}
	}

	return result, nil
}
func fieldToString(data interface{}) string {
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

// ToString converts a request to a string value
func (r *Request) ToString() string {
	var filters string
	for field, value := range r.Filters {
		filters += field.Name + ":" + fieldToString(value) + "#"
	}
	return fmt.Sprintf("%v->%v", r.ObjectID, filters)
}
