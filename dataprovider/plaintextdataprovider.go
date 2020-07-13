package dataprovider

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/ezeriver94/gotransform/common"
)

// PlainTextDataProvider is a dataprovider that can interact with text files, encoded in plain text
type PlainTextDataProvider struct {
	filePath string
	fields   []common.Field
	file     *os.File
	regexp   *regexp.Regexp
}

func getRegexp(fields []common.Field) (*regexp.Regexp, error) {
	regex := "^"
	var err error = nil
	for _, field := range fields {
		if field.FixedLength > 0 {
			regex += fmt.Sprintf("(.{%v})", field.FixedLength)
		} else if field.MaxLength > 0 && len(field.EndCharacter) == 1 {
			regex += fmt.Sprintf("(?:([^%v]{%v})|(?:([^%v]{0,%v})%v))", field.EndCharacter, field.MaxLength, field.EndCharacter, field.MaxLength-1, field.EndCharacter)
		} else {
			log.Print(fmt.Errorf("wrong field definition for %v. must have FixedLength or both MaxLength and EndCharacter", field.Name))
			if err == nil {
				err = errors.New("error on one or more dataEndpoint fields")
			}
		}
	}
	if err != nil {
		return nil, err
	}
	regex += "$"
	result, err := regexp.Compile(regex)
	return result, err
}

// Connect sets the filePath variable and checks if the requested path exists
func (dp *PlainTextDataProvider) Connect(connectionString, objectID string, fields []common.Field, connectionMode ConnectionMode) error {

	dp.fields = fields
	dp.filePath = connectionString
	regex, err := getRegexp(fields)
	if err != nil {
		return fmt.Errorf("error generating regex: %v", err)
	}
	dp.regexp = regex

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

func (dp *PlainTextDataProvider) parseRecord(text string) (map[common.Field]string, error) {
	result := make(map[common.Field]string, len(dp.fields))
	matchs := dp.regexp.FindSubmatch([]byte(text))

	if matchs == nil {
		return result, fmt.Errorf("text %v does not match regex %v", text, dp.regexp)
	}
	i := 0
	for _, match := range matchs[1:] {
		if match != nil {
			field := dp.fields[i]
			result[field] = string(match)
			i++
		}
	}
	return result, nil
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
		runeArrayValue := fieldToRuneArray(value)
		stringValue := string(runeArrayValue)
		if field.FixedLength > 0 {
			if len(runeArrayValue) > field.FixedLength {
				return result, fmt.Errorf("field %v has fixed length of %v and current value %v has longer length (%v)", field.Name, field.FixedLength, stringValue, len(runeArrayValue))
			}
			if len(runeArrayValue) == field.FixedLength {
				fieldValue = stringValue
			} else {
				if len(field.Padding.Char) != 1 {
					return result, fmt.Errorf("field %v has fixed length but padding character has not length of 1", field.Name)
				}
				if field.Padding.Mode == common.FieldPaddingLeft {
					fieldValue = strings.Repeat(field.Padding.Char, field.FixedLength-len(runeArrayValue)) + stringValue
				} else if field.Padding.Mode == common.FieldPaddingRight {
					fieldValue = stringValue + strings.Repeat(field.Padding.Char, field.FixedLength-len(runeArrayValue))
				}
			}
		} else {
			if len(field.EndCharacter) != 1 {
				return result, fmt.Errorf("field %v has no fixed length and end character has not length of 1", field.Name)
			}
			if len(runeArrayValue) > field.MaxLength {
				return result, fmt.Errorf("field %v has max length of %v and value %v is longer than that (%v)", field.Name, field.MaxLength, stringValue, len(runeArrayValue))
			}
			fieldValue = stringValue
			if len(runeArrayValue) < field.MaxLength {
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
		parsed, err := dp.parseRecord(text)
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
		parsed, err := dp.parseRecord(text)
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
