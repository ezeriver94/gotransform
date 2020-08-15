package phases

import (
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/ezeriver94/gotransform/common"
	"github.com/ezeriver94/gotransform/data"
)

// Loader handles loading data to a destination
type Loader struct {
	metadata  *common.Metadata
	records   map[string]chan common.Record
	wait      sync.WaitGroup
	accessors map[string]data.DataAccessor
}

// NewLoader creates a loader using the passed metadata
func NewLoader(metadata *common.Metadata) (Loader, error) {
	return Loader{
		metadata:  metadata,
		records:   make(map[string]chan common.Record),
		wait:      sync.WaitGroup{},
		accessors: make(map[string]data.DataAccessor),
	}, nil
}

// Initialize begins the saving of every provider that acts as a destination
func (l *Loader) Initialize() error {
	for key, target := range l.metadata.Load {
		accessor := data.NewDataAccessor(target.DataEndpoint.AccessorURL, key)

		l.accessors[key] = accessor
		if _, ok := l.records[target.TransformationName]; !ok {
			l.records[target.TransformationName] = make(chan common.Record)
		}
		l.wait.Add(1)
	}
	return nil
}

// Load the transformed data to a data endpoint
func (l *Loader) Load(record Transformed) {
	accessor, _ := l.accessors[record.TransformationName]
	accessor.Save(record.Record)
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
	return nil
}
