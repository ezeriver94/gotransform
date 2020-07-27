package dataprovider

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/ezeriver94/gotransform/common"
)

// Request contains the information needed for the dataprovider to fetch data
type Request struct {
	ObjectID string                       `json:"objectID"`
	Filters  map[common.Field]interface{} `json:"filters"`
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

// GetDataProviderFromDataSource returns a new dataprovider from the driver defined in the datasource
func GetDataProviderFromDataSource(metadata *common.Metadata, dataSourceName string, isPrimary bool) (DataProvider, error) {
	var dataSourcesMap map[string]common.DataEndpoint
	if isPrimary {
		dataSourcesMap = metadata.Extract.PrimaryDataSources
	} else {
		dataSourcesMap = metadata.Extract.AditionalDataSources
	}

	dataSource, ok := dataSourcesMap[dataSourceName]
	if !ok {
		var dataSourceType string
		if isPrimary {
			dataSourceType = "primary"
		} else {
			dataSourceType = "aditional"
		}
		return nil, fmt.Errorf("dataSource %v not found in the %v datasources map of metadata", dataSourceName, dataSourceType)
	}

	result, err := NewDataProvider(dataSource)
	if err != nil {
		log.Errorf("error creating dataprovider for driver %v", dataSource.Driver)
	}
	return result, nil
}

// NewDataProvider creates an instance of a dataprovider according to the driver passed as argument
func NewDataProvider(dataSource common.DataEndpoint) (DataProvider, error) {
	var result DataProvider
	var err error
	switch dataSource.Driver {
	case "plaintext":
		result, err = NewPlainTextDataProvider(dataSource)
	}

	return result, err
}

// ToString converts a request to a string value
func (r *Request) ToString() string {
	var filters string
	for field, value := range r.Filters {
		filters += field.Name + ":" + common.FieldToString(value) + "#"
	}
	return fmt.Sprintf("%v->%v", r.ObjectID, filters)
}

// HashCode returns a hashcode for a request
func (r Request) HashCode() string {
	var result string
	for field, value := range r.Filters {
		result += field.Name + "=" + fmt.Sprint(value) + "#"
	}
	return result
}
