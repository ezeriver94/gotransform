package common

import (
	"encoding/json"
	"sync"
)

// PrettyPrint returns a jsoned indented string
func PrettyPrint(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

type Key interface {
	HashCode() string
}

type Set interface {
	Add(Key)
	Remove(Key)
	Exists(Key) bool
	Length() int
	IsTheSame(*Key) bool
	Items() []*Key
}

type MapSet struct {
	entries       sync.Map
	addEntries    chan *Key
	removeEntries chan *Key
}

func NewSet() Set {
	result := MapSet{
		entries:       sync.Map{},
		addEntries:    make(chan *Key),
		removeEntries: make(chan *Key),
	}
	go result.listen()
	return &result
}
func (m *MapSet) IsTheSame(key *Key) bool {
	if result, ok := m.entries.Load((*key).HashCode()); ok {
		existing := result.(*Key)
		return existing == key
	}
	return false
}
func (m *MapSet) listen() {
	for {
		select {
		case entry := <-m.addEntries:
			m.entries.Store((*entry).HashCode(), entry)
		case entry := <-m.removeEntries:
			m.entries.Delete((*entry).HashCode())
		}
	}
}
func (m *MapSet) Add(key Key) {
	m.addEntries <- &key
}
func (m *MapSet) Remove(key Key) {
	m.removeEntries <- &key
}
func (s *MapSet) Exists(key Key) bool {
	_, ok := s.entries.Load(key)
	return ok
}

func (s *MapSet) Items() []*Key {
	result := make([]*Key, 0)
	s.entries.Range(func(key, value interface{}) bool {
		result = append(result, value.(*Key))
		return true
	})
	return result
}

func (s *MapSet) Length() int {
	return len(s.Items())
}
