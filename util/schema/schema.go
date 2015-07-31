package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	STRING = "string"
	INT    = "int"
	DOUBLE = "double"
	DATE   = "date"
)

type ColumnInfo struct {
	Name    string `json:"attr"`
	Type    string `json:"type"`
	Key     bool   `json:"key,omitempty"`
	Datefmt string `json:"datefmt,omitempty"`
	Default string `json:"default,omitempty"`
}

func (c *ColumnInfo) Pair() (pair string) {
	pair = fmt.Sprintf("'%s' %s", c.Name, c.Type)
	return
}

type Columns []*ColumnInfo

func (cc Columns) Attributes() (attr string) {
	pairs := make([]string, len(cc))
	for idx, info := range cc {
		pairs[idx] = info.Pair()
	}
	attr = strings.Join(pairs, ",")
	return
}

func (cc Columns) Keys() (keyattr string) {
	keys := make([]string, 0)
	for _, info := range cc {
		if info.Key {
			keys = append(keys, info.Name)
		}
	}
	if len(keys) > 0 {
		keyattr = "KEY(" + strings.Join(keys, ",") + ")"
	}
	return
}

type MetaData struct {
	Name    string      `json:"name"`
	Columns Columns     `json:"columns,omitempty"`
	Misc    interface{} `json:"misc,omitempty"`
}

func (m *MetaData) CreateStmt() (stmt string) {
	stmt = fmt.Sprintf("CREATE TABLE %s (%s %s)", m.Name, m.Columns.Attributes(), m.Columns.Keys())
	return
}

func (m *MetaData) TrimStmt() (stmt string) {
	stmt = fmt.Sprintf("TRIM %s to 0", m.Name)
	return
}

func (m *MetaData) EncodeJSON() (string, error) {
	metabuf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(metabuf).Encode(m); err != nil {
		return "", err
	} else {
		return metabuf.String(), nil
	}
}
