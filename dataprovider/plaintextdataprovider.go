package dataprovider

import (
	"bufio"
	"fmt"
	"os"

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
func ParseRecord

// Fetch finds a single value in the file which matches the filters in the request object and returns it if exists
func (dp PlainTextDataProvider) Fetch(r Request) (Record, error) {
	scanner := bufio.NewScanner(dp.file)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}
	return nil, nil
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
