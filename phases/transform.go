package phases

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/ezeriver94/gotransform/common"
	"github.com/ezeriver94/gotransform/data"
)

var TemporaryUnavailableJoin = errors.New("Missing join dependencies; leaving for now")

// Transformed is the result of a transformation applied
type Transformed struct {
	TransformationName string        `json:"transformationName"`
	Record             common.Record `json:"record"`
}

// Transformer handles transformations of an ETL job
type Transformer struct {
	metadata  *common.Metadata
	accessors map[string]data.DataAccessor
	sync      sync.Mutex
}

// NewTransformer creates a transformer using the passed metadata
func NewTransformer(metadata *common.Metadata) (Transformer, error) {
	return Transformer{
		metadata:  metadata,
		accessors: make(map[string]data.DataAccessor),
		sync:      sync.Mutex{},
	}, nil
}
func (t *Transformer) join(
	joins map[string]*common.Record,
	transformation common.DataTransformation,
	dataSourceName string,
	record *common.Record,
) (*common.Record, error) {

	result := common.NewRecord(false)
	if join, ok := joins[dataSourceName]; ok {
		return join, nil
	}

	join, ok := transformation.Joins[dataSourceName]
	if !ok {
		return &result, fmt.Errorf("join %v not found in metadata", dataSourceName)
	}
	targetJoinName := join.To
	targetJoin, ok := t.metadata.Extract.AditionalDataSources[targetJoinName]
	if !ok {
		return &result, fmt.Errorf("datasource %v not found in metadata", targetJoinName)
	}
	accessor, ok := t.accessors[targetJoinName]
	var err error
	if !ok {
		t.sync.Lock()
		// Re-check to handle accessor added after the check and before the lock
		accessor, ok = t.accessors[targetJoinName]
		if !ok {
			accessor = data.NewDataAccessor(targetJoin.AccessorURL, targetJoinName)
			if err != nil {
				return &result, fmt.Errorf("error building dataProvider for %v: %v", targetJoin.Driver, err)
			}
			if _, ok := t.accessors[dataSourceName]; !ok {
				t.accessors[dataSourceName] = accessor
			}
		}

		t.sync.Unlock()
	}
	filters := make(map[string]interface{})
	for _, onClause := range join.On {
		source, target, err := onClause.Parse()
		if err != nil {
			return &result, err
		}
		sourceName, sourceField, err := source.Parse()
		if err != nil {
			return &result, err
		}
		targetName, targetField, err := target.Parse()
		if err != nil {
			return &result, err
		}

		var (
			existingDataSourceName  string
			existingDataSourceField string
			pendingDataSourceField  string
		)
		if join.To != targetName && join.To != sourceName {
			return &result, fmt.Errorf("wrong join OnClause definition; neither one of the sources of the clause %v matches the target of the join %v", onClause, join.To)
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
			return &result, TemporaryUnavailableJoin
		}
		field, err := targetJoin.Fields.Find(pendingDataSourceField)
		if err != nil {
			return &result, err
		}
		if transformation.From == existingDataSourceName {
			data, err := record.Get(existingDataSourceField)
			if err != nil {
				return nil, fmt.Errorf("error finding value of field %v in record %v", existingDataSourceField, record)
			}
			filters[field.Name] = data
		} else {
			data, err := joins[existingDataSourceName].Get(existingDataSourceField)
			if err != nil {
				return nil, fmt.Errorf("error getting record value from key %v: %v", existingDataSourceField, err)
			}
			filters[field.Name] = data
		}
	}
	log.Debugf(record.Log("trying to join %v using %v filters", join.To, common.PrettyPrint(filters)))
	request := data.NewRequest(filters)
	joinedRecord, err := accessor.Fetch(request)
	if err != nil {
		return &result, fmt.Errorf("error fetching join record: %v", err)
	}
	return joinedRecord, nil
}

// Transform applies transformation rules to input fields of a datasource
func (t *Transformer) Transform(transformationName string, record *common.Record) (*Transformed, error) {
	log.Infof(record.Log("starting transformation for record %v", record))
	transformation, ok := t.metadata.Transform[transformationName]
	if !ok {
		return nil, fmt.Errorf(record.Log("invalid transformation with name %v in metadata", transformationName))
	}
	joins := make(map[string]*common.Record)
	fields := common.NewRecord(false)

	keepLooking := true

	for keepLooking {
		pendingKeys := make([]string, 0)
		for key := range transformation.Select {
			if ok, _ := fields.IsSet(key); !ok {
				pendingKeys = append(pendingKeys, key)
			}
		}

		for _, key := range pendingKeys {
			sel := transformation.Select[key]
			dataSourceName, fieldName, err := sel.Parse()
			if err != nil {
				return nil, err
			}

			if dataSourceName == transformation.From {
				value, err := record.Get(fieldName)
				if err != nil {
					return nil, fmt.Errorf("error finding value of field %v in record %v", fieldName, record)
				}
				fields.Set(key, value)
			} else {
				join, err := t.join(joins, transformation, dataSourceName, record)
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
				joinedRecord, _ := joins[dataSourceName]
				var value interface{}
				if joinedRecord.Empty {
					value = nil
				} else {
					value, err = joinedRecord.Get(fieldName)
					if err != nil {
						return nil, fmt.Errorf("error getting value of key %v for record %v", fieldName, joinedRecord)
					}
				}
				fields.Set(key, value)
			}
		}
		keepLooking = (len(transformation.Select) > fields.Length()) && (len(transformation.Select)-fields.Length() < len(pendingKeys))
	}
	if fields.Length() < len(transformation.Select) {
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
