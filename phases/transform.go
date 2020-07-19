package phases

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ezeriver94/gotransform/common"
	"github.com/ezeriver94/gotransform/dataprovider"
	"github.com/go-redis/cache/v8"
)

// Transformed is the result of a transformation applied
type Transformed struct {
	TransformationName string              `json:"transformationName"`
	Record             dataprovider.Record `json:"record"`
}

// Transformer handles transformations of an ETL job
type Transformer struct {
	metadata      *common.Metadata
	cache         *cache.Cache
	dataProviders map[string]dataprovider.DataProvider
	sync          sync.Mutex
}

// NewTransformer creates a transformer using the passed metadata
func NewTransformer(metadata *common.Metadata, cache *cache.Cache) (Transformer, error) {
	return Transformer{
		metadata:      metadata,
		cache:         cache,
		dataProviders: map[string]dataprovider.DataProvider{},
		sync:          sync.Mutex{},
	}, nil
}
func (t *Transformer) join() {

}

// Transform applies transformation rules to input fields of a datasource
func (t *Transformer) Transform(transformationName string, dataSourceFields map[string]interface{}) (*Transformed, error) {
	transformation, ok := t.metadata.Transform[transformationName]
	if !ok {
		return nil, fmt.Errorf("invalid transformation with name %v in metadata", transformationName)
	}
	joins := make(map[string]dataprovider.Record)
	fields := make(dataprovider.Record)

	keepLooking := true

	for keepLooking {
		pendingKeys := make([]string, 0)
		for key := range transformation.Select {
			if _, ok := fields[key]; !ok {
				pendingKeys = append(pendingKeys, key)
			}
		}

	SelectLoop:
		for _, key := range pendingKeys {
			sel := transformation.Select[key]

			dataSourceName, fieldName, err := sel.Parse()
			if err != nil {
				return nil, err
			}

			if dataSourceName == transformation.From {
				value, ok := dataSourceFields[fieldName]
				if !ok {
					return nil, fmt.Errorf("cannot find expected field %v in primary datasource values %v", fieldName, dataSourceFields)
				}
				fields[key] = value
			} else {
				joinedRecord, ok := joins[dataSourceName]
				if !ok {
					join, ok := transformation.Joins[dataSourceName]
					if !ok {
						return nil, fmt.Errorf("join %v not found in metadata", dataSourceName)
					}
					dataSourceName = join.To
					dataSource, ok := t.metadata.Extract.AditionalDataSources[dataSourceName]
					if !ok {
						return nil, fmt.Errorf("datasource %v not found in metadata", dataSourceName)
					}
					provider, ok := t.dataProviders[dataSourceName]
					if !ok {
						provider, err = dataprovider.NewDataProvider(dataSource)
						if err != nil {
							return nil, fmt.Errorf("error building dataProvider for %v: %v", dataSource.Driver, err)
						}
						err := provider.Connect(dataprovider.ConnectionModeRead)
						if err != nil {
							return nil, fmt.Errorf("error connecting to driver %v for datasource %v: %v", dataSource.Driver, dataSourceName, err)
						}
						t.sync.Lock()
						if _, ok := t.dataProviders[dataSourceName]; !ok {
							t.dataProviders[dataSourceName] = provider
						}
						t.sync.Unlock()

					}
					filters := make(map[common.Field]interface{})

					for _, onClause := range join.On {
						source, target, err := onClause.Parse()
						if err != nil {
							return nil, err
						}
						sourceName, sourceField, err := source.Parse()
						if err != nil {
							return nil, err
						}
						targetName, targetField, err := target.Parse()
						if err != nil {
							return nil, err
						}

						if sourceName == transformation.From && targetName == join.To {
							field, err := dataSource.Fields.Find(targetField)
							if err != nil {
								return nil, err
							}
							filters[field] = dataSourceFields[sourceField]
						} else if targetName == transformation.From && sourceName == join.To {
							field, err := dataSource.Fields.Find(sourceField)
							if err != nil {
								return nil, err
							}
							filters[field] = dataSourceFields[targetField]
						} else {
							if sourceName == join.To {
								field, err := dataSource.Fields.Find(sourceField)
								if err != nil {
									return nil, err
								}
								currentJoin, ok := joins[targetName]
								if !ok {
									log.Printf("could not find %v on existing joins; cant perform join %v. leaving join for now", sourceName, join.To)
									continue SelectLoop
								}
								filters[field] = currentJoin[targetField]
							} else if targetName == join.To {
								field, err := dataSource.Fields.Find(targetField)
								if err != nil {
									return nil, err
								}
								currentJoin, ok := joins[sourceName]
								if !ok {
									log.Printf("could not find %v on existing joins; cant perform join %v. leaving join for now", sourceName, join.To)
									continue SelectLoop
								}
								filters[field] = currentJoin[sourceField]
							} else {
								return nil, fmt.Errorf("wrong join OnClause definition; neither one of the sources of the clause %v matches the target of the join %v", onClause, join.To)
							}
						}
					}

					request := provider.NewRequest(filters)
					var matching dataprovider.Record
					if t.cache != nil {
						ctx := context.TODO()
						cacheKey := fmt.Sprintf("%v->%v", dataSourceName, request.ToString())

						if err := t.cache.Get(ctx, cacheKey, &matching); err != nil {
							log.Printf("cache miss for key %v. fetching data", cacheKey)
							matching, err = provider.Fetch(request)
							if err != nil {
								return nil, err
							}
							log.Printf("saving key %v with value %v in cache", cacheKey, matching)
							if err := t.cache.Set(&cache.Item{
								Ctx:   ctx,
								Key:   cacheKey,
								Value: matching,
								TTL:   time.Hour,
							}); err != nil {
								return nil, fmt.Errorf("error saving on cache %v", err)
							}
						} else {
							log.Printf("cache hit for key %v", cacheKey)
						}
					} else {
						matching, err = provider.Fetch(request)
						if err != nil {
							return nil, err
						}
					}

					joinedRecord = matching
					joins[dataSourceName] = matching
				}
				fields[key] = joinedRecord[fieldName]
			}
		}
		keepLooking = (len(transformation.Select) > len(fields)) && (len(transformation.Select)-len(fields) < len(pendingKeys))
	}
	if len(fields) < len(transformation.Select) {
		return nil, fmt.Errorf("error on transformation %v, could not perform every join expected", transformationName)
	}
	result := Transformed{
		TransformationName: transformationName,
		Record:             fields,
	}
	return &result, nil
}

// DeserializeTransformed deserializes a message to a Transformed instance
func DeserializeTransformed(data []byte) (Transformed, error) {
	var result Transformed
	err := json.Unmarshal(data, &result)
	return result, err
}

// Serialize converts a transformed result to a json string
func (t *Transformed) Serialize() ([]byte, error) {
	return json.Marshal(t)
}
