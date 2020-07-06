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

// DataSource is a DataEndpoint used as a source of a transformation
type DataSource struct {
	DataEndpoint
}

// DataDestination is a DataEndpoint used as a destination of a transformation
type DataDestination struct {
	DataEndpoint       `yaml:",inline"`
	TransformationName string `yaml:"transformation"`
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

// FieldPaddingMode Indicates the mode of the padding applied to a fixed-length field
type FieldPaddingMode string

const (
	// FieldPaddingLeft represents a left padded value when the data has fixed length
	FieldPaddingLeft FieldPaddingMode = "left"

	// FieldPaddingRight represents a right padded value when the data has fixed length
	FieldPaddingRight FieldPaddingMode = "right"
)

// FieldPadding defines the settings of the padding of a field
type FieldPadding struct {
	Mode FieldPaddingMode `yaml:"mode"`
	Char string           `yaml:"char"`
}

// Field represents the attributes of a field of the Metadata
type Field struct {
	Name         string       `yaml:"name"`
	ExpectedType string       `yaml:"type"`
	FixedLength  int          `yaml:"fixedlength"`
	MaxLength    int          `yaml:"maxlength"`
	EndCharacter string       `yaml:"endchar"`
	Padding      FieldPadding `yaml:"padding"`
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
	Load      map[string]DataDestination    `yaml:"load"`
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
