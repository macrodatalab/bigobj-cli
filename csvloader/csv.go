package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"unicode/utf8"
)

func NewCSVReader(filepath string, info *Schema) <-chan []string {
	input := make(chan []string, BulkSize)
	csvinfo := info.Misc.(*CSVMeta)
	go func() {
		var (
			file io.ReadCloser
			err  error
		)

		if filepath == "-" {
			file = os.Stdin
		} else {
			file, err = os.Open(filepath)
			if err != nil {
				log.Fatalln(err)
			}
			defer func() { file.Close() }()
		}

		// setup the complicated CSV parser
		r := csv.NewReader(file)
		r.Comma, _ = utf8.DecodeRuneInString(csvinfo.Comma)
		r.Comment, _ = utf8.DecodeRuneInString(csvinfo.Comment)
		r.FieldsPerRecord = csvinfo.Fields
		r.LazyQuotes = csvinfo.Quotes
		r.TrimLeadingSpace = csvinfo.Trim

		conv := NewDateConv(info.Columns)

		// skip through spcified lines
		for idx := int64(0); idx < csvinfo.SkipLines; idx++ {
			r.Read()
		}

		defer func() { recover() }() // we prepare for bad channel disrupt

		// continue to actual work
		for {
			record, err := r.Read()
			if err == io.EOF {
				close(input)
				return
			} else if err != nil {
				log.Println(err)
				continue
			}
			conv.Convert(record)
			input <- record
		}
	}()
	return input
}
