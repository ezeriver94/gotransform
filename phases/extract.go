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
	provider, err := dataprovider.GetDataProviderFromDataSource(e.metadata, dataSourceName, true)
	if err != nil {
		log.Print(err)
		return
	}
	err = provider.Connect(dataprovider.ConnectionModeRead)
	if err != nil {
		log.Print(fmt.Errorf("error connecting to datasource %v: %v", dataSourceName, err))
	}
	request := provider.NewRequest(nil)

	err = provider.Stream(request, records)
	if err != nil {
		log.Print(fmt.Errorf("error streaming datasource %v: %v", dataSourceName, err))
	} else {
		log.Printf("extraction for datasource %v finished successfully", dataSourceName)
	}
}
