package phases

import (
	"fmt"
	"log"

	"github.com/ezeriver94/gotransform/common"
	"github.com/ezeriver94/gotransform/dataprovider"
)

// Extractor parses all the primary datasources and streams every row into the channel
type Extractor struct {
	metadata *common.Metadata
}

// NewExtractor creates an extractor using the passed metadata
func NewExtractor(metadata *common.Metadata) (Extractor, error) {
	return Extractor{metadata: metadata}, nil
}

// Extract reads a
func (e *Extractor) Extract(dataSourceName string, records chan<- []interface{}) {
	dataSource, ok := e.metadata.Extract.PrimaryDataSources[dataSourceName]
	if !ok {
		log.Print(fmt.Errorf("dataSource %v not found in metadata", dataSourceName))
	}
	provider, err := dataprovider.NewDataProvider(dataSource.Driver)
	if err != nil {
		log.Print(fmt.Errorf("error creating dataprovider for driver %v", dataSource.Driver))
	}
	err = provider.Connect(dataSource.ConnectionString, dataSource.ObjectIdentifier, dataSource.Fields, dataprovider.ConnectionModeRead)
	if err != nil {
		log.Print(fmt.Errorf("error connecting to datasource %v: %v", dataSourceName, err))
	}
	request := dataprovider.NewRequest(dataSource.ObjectIdentifier, nil)
	err = provider.Stream(request, records)
	if err != nil {
		log.Print(fmt.Errorf("error streaming datasource %v: %v", dataSourceName, err))
	}
	log.Printf("extraction for datasource %v finished successfully", dataSourceName)
}
