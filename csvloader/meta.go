package main

import (
	"github.com/jeffjen/datefmt"
	"github.com/macrodatalab/bigobj-cli/util/schema"

	"bytes"
	"encoding/json"
	"log"
)

const (
	StdDateFmt string = `%Y-%m-%d %H:%M:%S`
)

type Schema schema.MetaData

func (meta *Schema) Set(val string) error {
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(val)))
	decoder.UseNumber()
	if err := decoder.Decode(meta); err != nil {
		log.Fatalln(err)
	}
	return nil
}

func (meta *Schema) String() string {
	return `{"name": "table_name", "columns": [{...},...], "misc": {...}}`
}

type CSVMeta struct {
	SkipLines int64  `json:"skip,omitempty"`
	Comma     string `json:"sep,omitempty"`
	Comment   string `json:"comment,omitempty"`
	Fields    int    `json:"fields,omitempty"`
	Quotes    bool   `json:"quotes,omitempty"`
	Trim      bool   `json:"trim,omitempty"`
}

func Decode(misc interface{}) (csvinfo *CSVMeta, err error) {
	csvinfo = &CSVMeta{
		defaultSkipLines,
		defaultComma,
		defaultComment,
		defaultFields,
		defaultLazyquotes,
		defaultTrimleadingspace,
	}
	defer func() {
		if bad := recover(); bad != nil {
			csvinfo, err = nil, bad.(error)
		}
	}()
	thing := misc.(map[string]interface{})
	for key, val := range thing {
		switch key {
		case "skip":
			csvinfo.SkipLines, _ = val.(json.Number).Int64()
			break
		case "sep":
			csvinfo.Comma = val.(string)
			break
		case "comment":
			csvinfo.Comment = val.(string)
			break
		case "fields":
			fields, _ := val.(json.Number).Int64()
			csvinfo.Fields = int(fields)
			break
		case "quotes":
			csvinfo.Quotes = val.(bool)
			break
		case "trim":
			csvinfo.Trim = val.(bool)
			break
		}
	}
	return
}

type DateFmtFunc func(string) string

type DateFmtHandle struct {
	fmtf []DateFmtFunc
}

func (h *DateFmtHandle) Convert(rec []string) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	for idx, f := range h.fmtf {
		rec[idx] = f(rec[idx])
	}
}

func NewDateConv(colinfo schema.Columns) (dfmth *DateFmtHandle) {
	dfmth = &DateFmtHandle{make([]DateFmtFunc, len(colinfo))}
	for idx, ci := range colinfo {
		if ci.Datefmt == "" {
			dfmth.fmtf[idx] = func(i string) string { return i }
		} else {
			var df string = ci.Datefmt
			dfmth.fmtf[idx] = func(i string) (o string) {
				t, _ := datefmt.Strptime(i, df)
				o, _ = datefmt.Strftime(t, StdDateFmt)
				return
			}
		}
	}
	return
}
