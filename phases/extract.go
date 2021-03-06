package phases

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/ezeriver94/gotransform/common"
	"github.com/ezeriver94/gotransform/data"
)

// Extractor parses all the primary datasources and streams every row into the channel
type Extractor struct {
	metadata *common.Metadata
}

// NewExtractor creates an extractor using the passed metadata
func NewExtractor(metadata *common.Metadata) (Extractor, error) {
	return Extractor{
		metadata: metadata,
	}, nil
}

// Extract reads every record of a dataSource and streams it into the records channel
func (e *Extractor) Extract(dataSourceName string, records chan<- common.Record) error {
	dataSource, ok := e.metadata.Extract.PrimaryDataSources[dataSourceName]
	if !ok {
		return fmt.Errorf("missing primary datasource %v on extract metadata", dataSourceName)
	}
	accessor := data.NewDataAccessor(dataSource.AccessorURL, dataSourceName)

	request := data.NewRequest(nil)

	err := accessor.Stream(records, request)
	if err != nil {
		return fmt.Errorf("error streaming datasource %v: %v", dataSourceName, err)
	}
	log.Infof("extraction for datasource %v finished successfully", dataSourceName)
	return nil

}
