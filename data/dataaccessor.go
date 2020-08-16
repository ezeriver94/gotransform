package data

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/ezeriver94/gotransform/cache"
	"github.com/ezeriver94/gotransform/common"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

type DataAccessor struct {
	Url *string
	ID  string "github.com/gorilla/websocket"
}

func NewDataAccessor(url, id string) DataAccessor {
	return DataAccessor{
		Url: flag.String("addr", url, "http service address"),
		ID:  id,
	}
}
func (da *DataAccessor) Save(r common.Record) error {
	u := url.URL{Scheme: "http", Host: *da.Url, Path: "/save"}

	jsonBody, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("error serializing request %v: %v", r, err)
	}
	log.Infof("saving %v to %v", string(jsonBody), u.String())

	resp, err := http.Post(u.String(), "application/json", bytes.NewBuffer(jsonBody))
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
	u := url.URL{Scheme: "http", Host: *da.Url, Path: "/fetch"}

	jsonBody, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("error serializing request %v: %v", r, err)
	}
	log.Infof("fetching %v from %v", string(jsonBody), u.String())

	cacheKey := fmt.Sprintf("%v->%v", da.ID, r.ToString())
	stringResult, err := cache.Retrieve(cacheKey, func() (interface{}, error) {
		resp, err := http.Post(u.String(), "application/json", bytes.NewBuffer(jsonBody))
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

func (da *DataAccessor) Stream(buffer chan<- common.Record, r Request) error {
	u := url.URL{Scheme: "ws", Host: *da.Url, Path: "/stream"}
	jsonBody, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("error serializing request %v: %v", r, err)
	}
	log.Infof("streaming %v from %v", string(jsonBody), u.String())
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)

	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			var record common.Record
			err := c.ReadJSON(&record)
			if err != nil {
				if _, k := err.(*websocket.CloseError); k {
					log.Infof("stream finished; returning control")
					return
				}

				log.Errorf("error reading message from websocket: %v", err)
				return
			}
			log.Infof("buffering record %v", record)
			buffer <- record

		}
	}()
	for {
		select {
		case <-done:
			return nil
		}
	}
	return nil
}
