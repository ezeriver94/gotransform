package dataprovider

import (
	"bufio"
	"fmt"
	"log"
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
func (dp *PlainTextDataProvider) Connect(connectionString, objectID string, fields []common.Field, connectionMode ConnectionMode) error {

	dp.fields = fields
	dp.filePath = connectionString
	var flag int
	if connectionMode == ConenctionModeWrite {
		_, err := os.Stat(connectionString)
		if err == nil {
			return fmt.Errorf("error: target file %v already exists", connectionString)
		}
		if os.IsNotExist(err) {
			_, err := os.Create(connectionString)
			if err != nil {
				return err
			}
		}
		flag = os.O_WRONLY
	} else {
		_, err := os.Stat(connectionString)
		if err != nil {
			return err
		}
		flag = os.O_RDONLY
	}

	file, err := os.OpenFile(dp.filePath, flag, os.ModeAppend)
	if err != nil {
		return fmt.Errorf("error opening file %v : %v", dp.filePath, err)
	}
	dp.file = file

	return nil
}

func parseRecord(text string, fields common.Fields) (map[common.Field]string, error) {
	nextToVisit := 0
	length := len(text)
	result := make(map[common.Field]string, len(fields))
	for _, field := range fields {
		if field.FixedLength > 0 {
			if length-nextToVisit < field.FixedLength {
				return nil, fmt.Errorf("error parsing line %v, expected length greater than %v and got %v ", text, nextToVisit+field.FixedLength, length)
			}
			result[field] = text[nextToVisit : nextToVisit+field.FixedLength]
			nextToVisit = nextToVisit + field.FixedLength
		} else if field.MaxLength > 0 && len(field.EndCharacter) > 0 {
			end := strings.Index(text[nextToVisit:], field.EndCharacter)
			if end == -1 {
				result[field] = text[nextToVisit : nextToVisit+field.MaxLength]
				nextToVisit = nextToVisit + field.MaxLength
			} else {
				index := math.Min(float64(end), float64(nextToVisit+field.MaxLength))
				result[field] = text[nextToVisit : nextToVisit+int(index)]
				nextToVisit += int(index) + 1
			}
		} else {
			return nil, fmt.Errorf("wrong field definition for %v. must have FixedLength or both MaxLength and EndCharacter", field.Name)
		}
	}
	return result, nil
}
func fieldToString(data interface{}) string {
	switch data.(type) {
	case bool:
		if data.(bool) == true {
			return "1"
		}
		return "0"
	default:
		if data == nil {
			return ""
		}
		return fmt.Sprint(data)
	}
}
func (r *Record) toString(fields common.Fields) (string, error) {
	result := ""
	for _, field := range fields {
		fieldName := field.Name
		value, ok := (*r)[fieldName]
		if !ok {
			return result, fmt.Errorf("cannot find field %v in target", fieldName)
		}
		var fieldValue string
		stringValue := fieldToString(value)
		if field.FixedLength > 0 {
			if len(stringValue) > field.FixedLength {
				return result, fmt.Errorf("field %v has fixed length of %v and current value %v has longer length (%v)", field.Name, field.FixedLength, stringValue, len(stringValue))
			}
			if len(stringValue) == field.FixedLength {
				fieldValue = stringValue
			} else {
				if len(field.Padding.Char) != 1 {
					return result, fmt.Errorf("field %v has fixed length but padding character has not length of 1", field.Name)
				}
				if field.Padding.Mode == common.FieldPaddingLeft {
					fieldValue = strings.Repeat(field.Padding.Char, field.FixedLength-len(stringValue)) + stringValue
				} else if field.Padding.Mode == common.FieldPaddingRight {
					fieldValue = stringValue + strings.Repeat(field.Padding.Char, field.FixedLength-len(stringValue))
				}
			}
		} else {
			if len(field.EndCharacter) != 1 {
				return result, fmt.Errorf("field %v has no fixed length and end character has not length of 1", field.Name)
			}
			if len(stringValue) > field.MaxLength {
				return result, fmt.Errorf("field %v has max length of %v and value %v is longer than that (%v)", field.Name, field.MaxLength, stringValue, len(stringValue))
			}
			fieldValue = stringValue
			if len(stringValue) < field.MaxLength {
				fieldValue += field.EndCharacter
			}
		}

		result += fieldValue
	}
	return result, nil
}

// Fetch finds a single value in the file which matches the filters in the request object and returns it if exists
func (dp *PlainTextDataProvider) Fetch(r Request) (Record, error) {
	scanner := bufio.NewScanner(dp.file)
	result := make(Record)
	var matches bool
	line := 0
	for scanner.Scan() {
		line++
		text := scanner.Text()
		parsed, err := parseRecord(text, dp.fields)
		if err != nil {
			return nil, fmt.Errorf("error parsing record %v, %v", text, err)
		}
		matches = true
		for filterField, filterValue := range r.Filters {
			matches = matches && string(parsed[filterField]) == fieldToString(filterValue)
		}
		if matches {
			for _, field := range dp.fields {
				validated, err := field.Validate(parsed[field])
				if err != nil {
					return nil, fmt.Errorf("found matching record on line %v but reached error validating record: %v", line, err)
				}
				result[field.Name] = validated
			}
			break
		}
	}
	return result, nil
}

// Stream reads every record in the file that matchs the filters in the request, and writes into the channel every one
func (dp *PlainTextDataProvider) Stream(r Request, buffer chan<- []interface{}) error {
	scanner := bufio.NewScanner(dp.file)
	var matches bool
	for scanner.Scan() {
		text := scanner.Text()
		if len(strings.TrimSpace(text)) == 0 {
			continue
		}
		parsed, err := parseRecord(text, dp.fields)
		if err != nil {
			return fmt.Errorf("error parsing record %v, %v", text, err)
		}
		matches = true
		for filterField, filterValue := range r.Filters {
			matches = matches && string(parsed[filterField]) == filterValue
		}
		if matches {
			record := make([]interface{}, len(dp.fields))
			for index, field := range dp.fields {
				record[index] = parsed[field]
			}
			buffer <- record
		}
	}
	close(buffer)
	return nil
}

// Save writes the data sent into de buffer to the connection
func (dp *PlainTextDataProvider) Save(buffer <-chan Record) error {
	// writer := bufio.NewWriter(dp.file)
	for {
		select {
		case record := <-buffer:
			strRecord, err := record.toString(dp.fields)
			if err != nil {
				log.Print(fmt.Errorf("error building string line from record: %v", err))
			}
			n, err := fmt.Fprintln(dp.file, strRecord)
			if err != nil {
				log.Print(fmt.Errorf("error writing line %v to file: %v", strRecord, err))
			}
			log.Printf("printed %v bytes to file ", n)
		}
	}
}

// Close closes the file (if it was opened)
func (dp *PlainTextDataProvider) Close() error {
	err := dp.file.Close()
	return err
}
