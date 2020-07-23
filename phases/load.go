package phases

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/ezeriver94/gotransform/common"
	"github.com/ezeriver94/gotransform/dataprovider"
)

// Loader handles loading data to a destination
type Loader struct {
	metadata  *common.Metadata
	records   map[string]chan dataprovider.Record
	wait      sync.WaitGroup
	providers map[string]dataprovider.DataProvider
}

// NewLoader creates a loader using the passed metadata
func NewLoader(metadata *common.Metadata) (Loader, error) {
	return Loader{
		metadata:  metadata,
		records:   make(map[string]chan dataprovider.Record),
		wait:      sync.WaitGroup{},
		providers: make(map[string]dataprovider.DataProvider),
	}, nil
}

// Initialize begins the saving of every provider that acts as a destination
func (l *Loader) Initialize() error {
	for key, target := range l.metadata.Load {
		provider, err := dataprovider.NewDataProvider(target.DataEndpoint)
		if err != nil {
			return fmt.Errorf("error building dataProvider %v for target %v from %v: %v", target.Driver, key, "", err)
		}
		err = provider.Connect(dataprovider.ConenctionModeWrite)
		if err != nil {
			return fmt.Errorf("error connecting to driver %v for target %v: %v", target.Driver, key, err)
		}
		l.providers[key] = provider
		if _, ok := l.records[target.TransformationName]; !ok {
			l.records[target.TransformationName] = make(chan dataprovider.Record)
		}
		l.wait.Add(1)
		go l.startSaving(provider, target)
	}
	return nil
}

func (l *Loader) startSaving(provider dataprovider.DataProvider, target common.DataDestination) error {
	defer l.wait.Done()
	provider.Save(l.records[target.TransformationName])
	return nil
}

// Load the transformed data to a data endpoint
func (l *Loader) Load(record Transformed) {
	l.records[record.TransformationName] <- record.Record
}

// Finish closes the loading channels and wait for every provider to finish writing
func (l *Loader) Finish() error {

	transformations := make(map[string]interface{}, len(l.metadata.Load))
	for _, target := range l.metadata.Load {
		transformationName := target.TransformationName
		if _, ok := transformations[transformationName]; !ok {
			log.Infof("closing channel for transformation %v", transformationName)
			close(l.records[transformationName])
			transformations[transformationName] = nil
		}
	}
	log.Infof("waiting for providers to finish their work")
	l.wait.Wait()
	log.Infof("providers finished working; proceeding to close providers")
	for key := range l.metadata.Load {
		provider := l.providers[key]
		log.Infof("closing provider %v", key)
		provider.Close()
	}

	return nil
}
