package common

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v2"
)

// Fields represents the set of fields of a datasource
type Fields []Field

// DataEndpoint contains information of a single entity which acts both as a source and as a data destination
type DataEndpoint struct {
	Driver           string `yaml:"driver"`
	ConnectionString string `yaml:"connectionstring"`
	ObjectIdentifier string `yaml:"objectid"`
	Fields           Fields `yaml:"fields"`
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

// OnClause represents a single on clause comparing two fields between datasources
type OnClause string

// SelectClause contains the way to obtain a value from a datasource. format: datasource.fieldname
type SelectClause string

// Join represents a way to join two datasources
type Join struct {
	To string     `yaml:"to"`
	On []OnClause `yaml:"on"`
}

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

// Parse returns the components of a select clause, splitting it by '.'
func (sc SelectClause) Parse() (string, string, error) {
	result := strings.Split(string(sc), ".")
	if len(result) == 2 {
		return strings.TrimSpace(result[0]), strings.TrimSpace(result[1]), nil
	}
	return "", "", fmt.Errorf("cannot split select clause %v", sc)
}

// Parse returns the components of an OnClause spitting it by '='
func (oc OnClause) Parse() (SelectClause, SelectClause, error) {
	result := strings.Split(string(oc), "=")
	if len(result) == 2 {
		return SelectClause(result[0]), SelectClause(result[1]), nil
	}
	return "", "", fmt.Errorf("cannot split on clause %v", oc)
}

// Find tries to find a named field inside the array
func (f Fields) Find(name string) (Field, error) {

	for _, field := range f {
		if field.Name == name {
			return field, nil
		}
	}
	var result Field
	return result, fmt.Errorf("field %v not found", name)
}
