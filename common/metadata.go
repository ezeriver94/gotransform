package common

import (
	"fmt"

	"gopkg.in/yaml.v2"
)

// DataEndpoint contains information of a single entity which acts both as a source and as a data destination
type DataEndpoint struct {
	Driver           string  `yaml:"driver"`
	ConnectionString string  `yaml:"connectionstring"`
	ObjectIdentifier string  `yaml:"objectid"`
	Fields           []Field `yaml:"fields"`
}

// Join represents a way to join two datasources
type Join struct {
	To string   `yaml:"to"`
	On []string `yaml:"on"`
}

// SelectClause contains the way to obtain a value from a datasource. format: datasource.fieldname
type SelectClause string

// DataTransformation defines the directives to use to handle a transformation on one or multiple datasources
type DataTransformation struct {
	From   string                  `yaml:"from"`
	Joins  map[string]Join         `yaml:"joins"`
	Where  []string                `yaml:"where"`
	Select map[string]SelectClause `yaml:"select"`
}

// Field represents the attributes of a field of the Metadata
type Field struct {
	Name         string `yaml:"name"`
	ExpectedType string `yaml:"type"`
	FixedLength  int    `yaml:"fixedlength"`
	MaxLength    int    `yaml:"maxlength"`
	EndCharacter string `yaml:"endchar"`
}

// Extract contains primary datasources (which are fully read and cannot be related to each others) and aditional datasources (used on join clauses on transformations)
type Extract struct {
	PrimaryDataSources   map[string]DataEndpoint `yaml:"primary"`
	AditionalDataSources map[string]DataEndpoint `yaml:"aditional"`
}

// Metadata represents the definition of a single row of data
type Metadata struct {
	Version   string                        `yaml:"version"`
	Extract   Extract                       `yaml:"extract"`
	Transform map[string]DataTransformation `yaml:"transform"`
	Load      map[string]DataEndpoint       `yaml:"load"`
}

// ParseMetadata converts a string to a Metadata instance
func ParseMetadata(data string) (*Metadata, error) {
	var result = new(Metadata)
	err := yaml.UnmarshalStrict([]byte(data), &result)
	if err != nil {
		return nil, fmt.Errorf("error deserializing metadata as yaml: %v", err)
	}
	return result, nil
}
