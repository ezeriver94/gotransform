package data

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/ezeriver94/gotransform/cache"
	"github.com/ezeriver94/gotransform/common"
)

type DataAccessor struct {
	Url string
	ID  string
}

func NewDataAccessor(url, id string) DataAccessor {
	return DataAccessor{
		Url: url,
		ID:  id,
	}
}
func (da *DataAccessor) Save(r common.Record) error {
	jsonBody, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("error serializing request %v: %v", r, err)
	}
	resp, err := http.Post(da.Url, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("error fetching data with request %v: %v", r, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("error fetching data with request %v: %v", r, err)
	}

	return nil
}
func (da *DataAccessor) Fetch(r Request) (*common.Record, error) {
	jsonBody, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("error serializing request %v: %v", r, err)
	}
	cacheKey := fmt.Sprintf("%v->%v", da.ID, r.ToString())
	stringResult, err := cache.Retrieve(cacheKey, func() (interface{}, error) {
		resp, err := http.Post(da.Url, "application/json", bytes.NewBuffer(jsonBody))
		if err != nil {
			return nil, fmt.Errorf("error fetching data with request %v: %v", r, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("error fetching data with request %v: %v", r, err)
		}

		resultJSON, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading response body: %v", err)
		}
		return resultJSON, nil
	})
	if err != nil {
		return nil, fmt.Errorf("error finding join value: %v", err)
	}
	log.Infof("found join value for join %v: %v", r, stringResult)
	var result common.Record

	err = json.Unmarshal([]byte(stringResult), &result) //result.PopulateFromJSON(stringResult)
	if err != nil {
		return nil, fmt.Errorf("error deserializing string to record %v: %v", stringResult, err)
	}
	return &result, nil
}

func (da *DataAccessor) Stream(buffer chan<- *common.Record, r Request) error {
	// WEBSOCKET
	return nil
}
