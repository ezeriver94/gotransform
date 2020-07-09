package phases

import (
	"encoding/json"
	"fmt"

	"github.com/ezeriver94/gotransform/common"
	"github.com/ezeriver94/gotransform/dataprovider"
)

// Transformed is the result of a transformation applied
type Transformed map[string]interface{}

// Transformer handles transformations of an ETL job
type Transformer struct {
	metadata *common.Metadata
}

// NewTransformer creates a transformer using the passed metadata
func NewTransformer(metadata *common.Metadata) (Transformer, error) {
	return Transformer{metadata: metadata}, nil
}

// Transform applies transformation rules to input fields of a datasource
func (t *Transformer) Transform(transformationName string, dataSourceFields map[string]interface{}) (Transformed, error) {
	transformation, ok := t.metadata.Transform[transformationName]
	if !ok {
		return nil, fmt.Errorf("invalid transformation with name %v in metadata", transformationName)
	}
	providers := make(map[string]dataprovider.DataProvider)
	result := make(Transformed)

	for key, sel := range transformation.Select {
		dataSourceName, field, err := sel.Parse()
		if err != nil {
			return nil, err
		}
		var value interface{}

		if dataSourceName == transformation.From {
			var ok bool
			value, ok = dataSourceFields[field]
			if !ok {
				return nil, fmt.Errorf("cannot find expected field %v in primary datasource values %v", field, dataSourceFields)
			}
		} else {
			join, ok := transformation.Joins[dataSourceName]
			if !ok {
				return nil, fmt.Errorf("join %v not found in metadata", dataSourceName)
			}
			dataSourceName = join.To
			dataSource, ok := t.metadata.Extract.AditionalDataSources[dataSourceName]
			if !ok {
				return nil, fmt.Errorf("datasource %v not found in metadata", dataSourceName)
			}
			provider, ok := providers[dataSourceName]
			if !ok {
				provider, err = dataprovider.NewDataProvider(dataSource.Driver)
				if err != nil {
					return nil, fmt.Errorf("error building dataProvider from %v: %v", "", err)
				}
				err := provider.Connect(dataSource.ConnectionString, dataSource.ObjectIdentifier, dataSource.Fields)
				if err != nil {
					return nil, fmt.Errorf("error connecting to driver %v for datasource %v: %v", dataSource.Driver, dataSourceName, err)
				}

				providers[dataSourceName] = provider
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
					// TODO: contemplar joins previos
				}
			}

			request := dataprovider.NewRequest(dataSource.ObjectIdentifier, filters)
			matching, err := provider.Fetch(request)
			if err != nil {
				return nil, err
			}
			value = matching[field]
		}
		result[key] = value
	}

	return result, nil
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
