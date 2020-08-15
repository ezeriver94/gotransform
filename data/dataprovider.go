package data

import (
	"fmt"

	"github.com/ezeriver94/gotransform/common"
)

// Request contains the information needed for the dataprovider to fetch data
type Request struct {
	ObjectID string                 `json:"objectID"`
	Filters  map[string]interface{} `json:"filters"`
}

// ConnectionMode indicates the type of connection
type ConnectionMode int

const (
	ConnectionModeRead ConnectionMode = iota
	ConenctionModeWrite
)

// DataProvider is a class that can perform actions against a datasource (fetching and saving data)
type DataProvider interface {
	Connect(connectionMode ConnectionMode) error

	NewRequest(filters map[common.Field]interface{}) Request
	Fetch(r Request) (*common.Record, error)

	Stream(r Request, buffer chan<- *common.Record) error
	Save(buffer <-chan common.Record) error

	Close() error
}

// NewRequest creates a new request for the plaintext dataprovider
func NewRequest(filters map[string]interface{}) Request {
	return Request{
		Filters: filters,
	}
}

// ToString converts a request to a string value
func (r *Request) ToString() string {
	var filters string
	for field, value := range r.Filters {
		filters += field + ":" + common.FieldToString(value) + "#"
	}
	return fmt.Sprintf("%v->%v", r.ObjectID, filters)
}

// HashCode returns a hashcode for a request
func (r Request) HashCode() string {
	var result string
	for field, value := range r.Filters {
		result += field + "=" + fmt.Sprint(value) + "#"
	}
	return result
}
