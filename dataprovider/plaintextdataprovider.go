package dataprovider

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/beevik/guid"
	"github.com/ezeriver94/gotransform/common"
)

// Fetched represents a key value struct with a record obtained by filtering some request which transformed to string returns "key"
type Fetched struct {
	key   string
	value Record
}

// PlainTextDataProvider is a dataprovider that can interact with text files, encoded in plain text
type PlainTextDataProvider struct {
	objectID string
	filePath string
	fields   []common.Field
	file     *os.File
	regexp   *regexp.Regexp

	found    chan Fetched
	requests []Request
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

// NewPlainTextDataProvider creates a new plain text data provider from the dataEndpoint information
func NewPlainTextDataProvider(dataEndpoint common.DataEndpoint) (*PlainTextDataProvider, error) {
	result := PlainTextDataProvider{
		filePath: dataEndpoint.ConnectionString,
		fields:   dataEndpoint.Fields,
		objectID: dataEndpoint.ObjectIdentifier,
		found:    make(chan Fetched),
		requests: make([]Request, 0),
	}

	regex, err := getRegexp(dataEndpoint.Fields)
	if err != nil {
		return nil, fmt.Errorf("error generating regex: %v", err)
	}
	result.regexp = regex

	return &result, nil
}

// Connect sets the filePath variable and checks if the requested path exists
func (dp *PlainTextDataProvider) Connect(connectionMode ConnectionMode) error {

	var flag int
	if connectionMode == ConenctionModeWrite {
		_, err := os.Stat(dp.filePath)
		if err == nil {
			return fmt.Errorf("error: target file %v already exists", dp.filePath)
		}
		if os.IsNotExist(err) {
			_, err := os.Create(dp.filePath)
			if err != nil {
				return err
			}
		}
		flag = os.O_WRONLY
	} else {
		_, err := os.Stat(dp.filePath)
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

func (dp *PlainTextDataProvider) beginQuest() {
	pool := sync.Pool{}

	for {
		select {
		case _, more := <-dp.found:
			if !more {
				break
			}
		default:
		}
		for len(dp.requests) > 0 {

		}
	}
}

func (dp *PlainTextDataProvider) read(offset int64, limit int64, fileName string, channel chan (string)) {
	file, err := os.Open(fileName)
	defer file.Close()

	if err != nil {
		panic(err)
	}

	// Move the pointer of the file to the start of designated chunk.
	file.Seek(offset, 0)
	reader := bufio.NewReader(file)

	// This block of code ensures that the start of chunk is a new word. If
	// a character is encountered at the given position it moves a few bytes till
	// the end of the word.
	if offset != 0 {
		_, err = reader.ReadBytes(' ')
		if err == io.EOF {
			fmt.Println("EOF")
			return
		}

		if err != nil {
			panic(err)
		}
	}

	var cummulativeSize int64
	for {
		// Break if read size has exceed the chunk size.
		if cummulativeSize > limit {
			break
		}

		b, err := reader.ReadBytes('\n')

		// Break if end of file is encountered.
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		cummulativeSize += int64(len(b))
		s := string(b)
		parsed, err := dp.parseRecord(s)
		if err != nil {
			log.Print(fmt.Errorf("error parsing record %v, %v", s, err))
			continue
		}
		for _, req := range dp.requests {
			matches := true
			for filterField, filterValue := range req.Filters {
				matches = matches && string(parsed[filterField]) == fieldToString(filterValue)
			}
			if matches {
				var record Record
				log.Printf("record %v matches filter of %v; join ended", parsed, req)
				for _, field := range dp.fields {
					validated, err := field.Validate(parsed[field])
					if err != nil {
						log.Print(fmt.Errorf("found matching record on line %v but reached error validating record: %v", s, err))
					}
					record[field.Name] = validated
				}
				dp.found <- Fetched{
					key:   req.ToString(),
					value: record,
				}
				// dp.requests
				break
			} else {
				log.Printf("record %v dont matches filter of %v", parsed, req)
			}
		}

		// if s != "" {
		// 	// Send the read word in the channel to enter into dictionary.
		// 	channel <- s
		// }
	}
}

// Fetch finds a single value in the file which matches the filters in the request object and returns it if exists
func (dp *PlainTextDataProvider) Fetch(r Request) (Record, error) {
	guid := guid.NewString()

	scanner := bufio.NewScanner(dp.file)
	result := make(Record)
	var matches bool
	line := 0
	log.Printf("GUID %v: starting search for %v by scanning file %v", r, guid, dp.filePath)

	for scanner.Scan() {
		line++
		text := scanner.Text()
		log.Printf("GUID %v: does %v matches %v ?", guid, text, r)
		parsed, err := dp.parseRecord(text)
		if err != nil {
			return nil, fmt.Errorf("GUID %v: error parsing record %v, %v", guid, text, err)
		}
		matches = true
		for filterField, filterValue := range r.Filters {
			matches = matches && string(parsed[filterField]) == fieldToString(filterValue)
		}
		if matches {
			log.Printf("GUID %v: record %v matches filter of %v; join ended", guid, parsed, r)
			for _, field := range dp.fields {
				validated, err := field.Validate(parsed[field])
				if err != nil {
					return nil, fmt.Errorf("GUID %v: found matching record on line %v but reached error validating record: %v", guid, line, err)
				}
				result[field.Name] = validated
			}
			break
		} else {
			log.Printf("GUID %v: record %v dont matches filter of %v", guid, parsed, r)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("GUID %v: error scanning file: %v", guid, err)
	}

	if !matches {
		log.Printf("GUID %v: no record matches with filter %v", guid, r)
	}
	return result, nil
}
func (dp *PlainTextDataProvider) streamRecord(record string, req Request, buffer chan<- []interface{}, wait *sync.WaitGroup) {
	defer wait.Done()
	if len(strings.TrimSpace(record)) == 0 {
		return
	}
	parsed, err := dp.parseRecord(record)
	if err != nil {
		log.Print(fmt.Errorf("error parsing record %v, %v", record, err))
		return
	}
	matches := true
	for filterField, filterValue := range req.Filters {
		matches = matches && string(parsed[filterField]) == filterValue
	}
	if matches {
		result := make([]interface{}, len(dp.fields))
		for index, field := range dp.fields {
			result[index] = parsed[field]
		}
		buffer <- result
	}
}

// Stream reads every record in the file that matchs the filters in the request, and writes into the channel every one
func (dp *PlainTextDataProvider) Stream(r Request, buffer chan<- []interface{}) error {
	wait := sync.WaitGroup{}

	scanner := bufio.NewScanner(dp.file)
	for scanner.Scan() {
		text := scanner.Text()
		wait.Add(1)
		go dp.streamRecord(text, r, buffer, &wait)
	}
	wait.Wait()
	return nil
}

// Save writes the data sent into de buffer to the connection
func (dp *PlainTextDataProvider) Save(buffer <-chan Record) error {
	for {
		select {
		case record, more := <-buffer:
			if record != nil {
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
			if !more {
				return nil
			}
		}
	}
}

// Close closes the file (if it was opened)
func (dp *PlainTextDataProvider) Close() error {
	err := dp.file.Close()
	return err
}

// NewRequest creates a new request for the plaintext dataprovider
func (dp *PlainTextDataProvider) NewRequest(filters map[common.Field]interface{}) Request {
	return Request{
		ObjectID: dp.objectID,
		Filters:  filters,
	}
}
