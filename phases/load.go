package phases

import (
	"fmt"
	"log"

	"github.com/ezeriver94/gotransform/common"
	"github.com/ezeriver94/gotransform/dataprovider"
)

// Loader handles loading data to a destination
type Loader struct {
	metadata *common.Metadata
}

// NewLoader creates a transformer using the passed metadata
func NewLoader(metadata *common.Metadata) (Loader, error) {
	return Loader{metadata: metadata}, nil
}

// Load the transformed data to a data endpoint
func (l *Loader) Load(transforms <-chan Transformed) error {
	providers := make(map[string]dataprovider.DataProvider)
	records := make(chan dataprovider.Record)
	for key, target := range l.metadata.Load {
		provider, err := dataprovider.NewDataProvider(target.DataEndpoint.Driver)
		if err != nil {
			return fmt.Errorf("error building dataProvider %v for target %v from %v: %v", target.Driver, key, "", err)
		}
		err = provider.Connect(target.DataEndpoint.ConnectionString, target.DataEndpoint.ObjectIdentifier, target.DataEndpoint.Fields, dataprovider.ConenctionModeWrite)
		if err != nil {
			return fmt.Errorf("error connecting to driver %v for target %v: %v", target.Driver, key, err)
		}
		providers[key] = provider
		go provider.Save(records)
	}
	closed := false
	for {
		select {
		case transformedRecord, more := <-transforms:
			records <- transformedRecord.Record
			if !more {
				closed = true
			}
		}
		if closed {
			break
		}
	}
	close(records)
	for key, provider := range providers {
		log.Printf("closing provider %v", key)
		provider.Close()
	}
	return nil
}
