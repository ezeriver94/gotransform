package dataprovider

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/ezeriver94/gotransform/common"
)

// PlainTextDataProvider is a dataprovider that can interact with text files, encoded in plain text
type PlainTextDataProvider struct {
	filePath string
	fields   []common.Field
	file     *os.File
}

// Connect sets the filePath variable and checks if the requested path exists
func (dp PlainTextDataProvider) Connect(conectionString, objectID string, fields []common.Field) error {
	_, err := os.Stat(conectionString)
	if err != nil {
		return err
	}
	dp.fields = fields
	dp.filePath = conectionString
	file, err := os.Open(dp.filePath)
	if err != nil {
		return fmt.Errorf("error opening file %v : %v", dp.filePath, err)
	}
	dp.file = file

	return nil
}

func parseRecord(text string, fields common.Fields) (map[common.Field]string, error) {
	lastVisited := 0
	length := len(text)
	result := make(map[common.Field]string, len(fields))
	for _, field := range fields {
		if field.FixedLength > 0 {
			if length-lastVisited < field.FixedLength {
				return nil, fmt.Errorf("error parsing line %v, expected length greater than %v and got %v ", text, lastVisited+field.FixedLength, length)
			}
			result[field] = text[lastVisited : lastVisited+field.FixedLength]
			lastVisited = lastVisited + field.FixedLength - 1
		} else if field.MaxLength > 0 && len(field.EndCharacter) > 0 {
			end := strings.Index(text[lastVisited:], field.EndCharacter)
			if end == -1 {
				result[field] = text[lastVisited : lastVisited+field.MaxLength]
				lastVisited = lastVisited + field.MaxLength - 1
			} else {
				index := math.Min(float64(end), float64(lastVisited+field.MaxLength))
				result[field] = text[lastVisited:int(index)]
				lastVisited = int(index) - 1
			}
		} else {
			return nil, fmt.Errorf("wrong field definition for %v. must have FixedLength or both MaxLength and EndCharacter", field.Name)
		}
	}
	return result, nil
}

// Fetch finds a single value in the file which matches the filters in the request object and returns it if exists
func (dp PlainTextDataProvider) Fetch(r Request) (Record, error) {
	scanner := bufio.NewScanner(dp.file)
	result := make(Record)
	for scanner.Scan() {
		text := scanner.Text()
		parsed, err := parseRecord(text, dp.fields)
		if err != nil {
			return nil, fmt.Errorf("error parsing record %v, %v", text, err)
		}
		for filterField, filterValue := range r.Filters {
			if parsed[filterField] == filterValue {
				break
			}
		}
	}

	return result, nil
}

// Stream reads every record in the file that matchs the filters in the request, and writes into the channel every one
func (dp PlainTextDataProvider) Stream(r Request, buffer chan<- Record) error {
	return nil
}

// Save writes the data sent into de buffer to the connection
func (dp PlainTextDataProvider) Save(buffer <-chan Record) error {
	return nil
}

// Close closes the file (if it was opened)
func (dp PlainTextDataProvider) Close() error {
	return nil
}
