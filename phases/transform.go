package phases

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/ezeriver94/gotransform/cache"
	"github.com/ezeriver94/gotransform/common"
	"github.com/ezeriver94/gotransform/dataprovider"
)

var TemporaryUnavailableJoin = errors.New("Missing join dependencies; leaving for now")

// Transformed is the result of a transformation applied
type Transformed struct {
	TransformationName string              `json:"transformationName"`
	Record             dataprovider.Record `json:"record"`
}

// Transformer handles transformations of an ETL job
type Transformer struct {
	metadata      *common.Metadata
	cache         *cache.KeyValueCache
	dataProviders map[string]dataprovider.DataProvider
	sync          sync.Mutex
}

// NewTransformer creates a transformer using the passed metadata
func NewTransformer(metadata *common.Metadata, cache *cache.KeyValueCache) (Transformer, error) {
	return Transformer{
		metadata:      metadata,
		cache:         cache,
		dataProviders: map[string]dataprovider.DataProvider{},
		sync:          sync.Mutex{},
	}, nil
}
func (t *Transformer) fetchJoin(
	provider dataprovider.DataProvider,
	filters map[common.Field]interface{},
	dataSourceName string,
) (dataprovider.Record, error) {

	request := provider.NewRequest(filters)
	cacheKey := fmt.Sprintf("%v->%v", dataSourceName, request.ToString())

	fetchRequest := func() (interface{}, error) {
		return provider.Fetch(request)
	}
	stringResult, err := t.cache.Retrieve(cacheKey, fetchRequest)
	if err != nil {
		return nil, fmt.Errorf("error finding join value: %v", err)
	}
	log.Infof("found join value for join %v: %v", request, stringResult)
	var result dataprovider.Record
	err = json.Unmarshal([]byte(stringResult), &result)
	if err != nil {
		return nil, fmt.Errorf("error deserializing value %v", stringResult)
	}
	return result, nil
}

func (t *Transformer) join(
	joins map[string]dataprovider.Record,
	transformation common.DataTransformation,
	dataSourceName string,
	dataSourceFields map[string]interface{},
) (dataprovider.Record, error) {

	if join, ok := joins[dataSourceName]; ok {
		return join, nil
	}

	join, ok := transformation.Joins[dataSourceName]
	if !ok {
		return nil, fmt.Errorf("join %v not found in metadata", dataSourceName)
	}
	targetJoinName := join.To
	targetJoin, ok := t.metadata.Extract.AditionalDataSources[targetJoinName]
	if !ok {
		return nil, fmt.Errorf("datasource %v not found in metadata", targetJoinName)
	}
	provider, ok := t.dataProviders[targetJoinName]
	var err error
	if !ok {
		provider, err = dataprovider.NewDataProvider(targetJoin)
		if err != nil {
			return nil, fmt.Errorf("error building dataProvider for %v: %v", targetJoin.Driver, err)
		}
		err = provider.Connect(dataprovider.ConnectionModeRead)
		if err != nil {
			return nil, fmt.Errorf("error connecting to driver %v for datasource %v: %v", targetJoin.Driver, targetJoinName, err)
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

		var (
			existingDataSourceName  string
			existingDataSourceField string
			pendingDataSourceField  string
		)
		if join.To != targetName && join.To != sourceName {
			return nil, fmt.Errorf("wrong join OnClause definition; neither one of the sources of the clause %v matches the target of the join %v", onClause, join.To)
		}
		if _, ok := joins[sourceName]; ok || sourceName == transformation.From {
			existingDataSourceName = sourceName
			existingDataSourceField = sourceField
			pendingDataSourceField = targetField
		} else if _, ok := joins[targetName]; ok || targetName == transformation.From {
			existingDataSourceName = targetName
			existingDataSourceField = targetField
			pendingDataSourceField = sourceField
		} else {
			log.Debugf("could not find %v on existing joins; cant perform join %v. leaving join for now", sourceName, join.To)
			return nil, TemporaryUnavailableJoin
		}
		field, err := targetJoin.Fields.Find(pendingDataSourceField)
		if err != nil {
			return nil, err
		}
		if transformation.From == existingDataSourceName {
			filters[field] = dataSourceFields[existingDataSourceField]
		} else {
			filters[field] = joins[existingDataSourceName][existingDataSourceField]
		}
	}
	log.Debugf("trying to join %v using %v filters", join.To, common.PrettyPrint(filters))
	joinedRecord, err := t.fetchJoin(provider, filters, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("error fetching join record: %v", err)
	}
	return joinedRecord, nil
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
				join, err := t.join(joins, transformation, dataSourceName, dataSourceFields)
				if err != nil {
					if err == TemporaryUnavailableJoin {
						continue
					}
					return nil, fmt.Errorf("error joining record: %v", err)
				}
				joins[dataSourceName] = join
				if err != nil {
					return nil, fmt.Errorf("error on transformation join: %v", err)
				}
				joinedRecord, ok := joins[dataSourceName]
				if !ok {
					continue SelectLoop
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
