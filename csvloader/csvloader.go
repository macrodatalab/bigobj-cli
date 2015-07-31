package main

import (
	"github.com/macrodatalab/bigobj-cli/util/api"
	"github.com/macrodatalab/bigobj-cli/util/logging"

	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	defaultHost = "localhost"
	defaultPort = "9090"

	// default specification for csv file
	defaultSkipLines        = 0
	defaultComma            = ","
	defaultComment          = "#"
	defaultFields           = -1
	defaultLazyquotes       = true
	defaultTrimleadingspace = true

	// default size for bulk insert size
	defaultBulkSize = 10000

	// default behavior for outputing request
	defaultDebug = false
)

var (
	// location of our bigobject service daemon
	Host string
	Port string

	// schema information about the data
	Info Schema

	// path to the CSV file to load
	FPath string

	// bulk insert size
	BulkSize int

	// to dump the request or not
	Debug bool

	// logger for the formulated request
	StmtLog logging.Logger
)

func init() {
	var (
		// table name for this load session
		table string

		// CSV schema i.e. linkes to skip, separator, quote rule, etc
		csvinfo CSVMeta
	)
	flag.StringVar(&Host, "H", defaultHost, "Provide hostname to bigobject")
	flag.StringVar(&Port, "P", defaultPort, "Provide port to bigobject")
	flag.BoolVar(&Debug, "D", defaultDebug, "Debug csv parsing the request formation")
	flag.StringVar(&FPath, "F", "", "Provide path to CSV file to load, '-' for stdin")
	flag.StringVar(&table, "T", "", "Provide tavble name to load into")
	flag.IntVar(&BulkSize, "B", defaultBulkSize, "How many lines will be batched together")
	flag.Int64Var(&csvinfo.SkipLines, "skip", int64(defaultSkipLines), "Lines to skip during load")
	flag.StringVar(&csvinfo.Comma, "sep", defaultComma, "What is the field separator")
	flag.StringVar(&csvinfo.Comment, "comment", defaultComment, "What is the comment indicator")
	flag.IntVar(&csvinfo.Fields, "fields", defaultFields, "How many fields in a record")
	flag.BoolVar(&csvinfo.Quotes, "quotes", defaultLazyquotes, "You are not too picky about quotes")
	flag.BoolVar(&csvinfo.Trim, "trim", defaultTrimleadingspace, "Firt field white space removed")
	flag.Var(&Info, "metadata", "Optional metadata information for type assertion and how to interpret CSV")
	flag.Parse()

	if FPath == "" {
		log.Fatalln("Abort no file to load")
	}

	if table != "" {
		Info.Name = table
	} else if Info.Name == "" {
		log.Fatalln("Abort destination table unspecified")
	}

	if Info.Misc == nil {
		Info.Misc = &csvinfo
	} else {
		if cinfo, err := Decode(Info.Misc); err != nil {
			log.Fatalln(err)
		} else {
			Info.Misc = cinfo
		}
	}

	StmtLog = logging.NewLogger(Debug)
}

func Quote(record []string) (q []string) {
	q = make([]string, len(record))
	for idx := 0; idx < len(record); idx++ {
		q[idx] = strconv.Quote(record[idx])
	}
	return
}

func BuildRequestFromCSV(filepath string, info *Schema, bulksize int) io.ReadCloser {
	r, w := io.Pipe()
	go func() {
		defer func() { w.Close() }()
		buf := make([]string, 0)
		for line := range NewCSVReader(filepath, info) {
			line = Quote(line)
			buf = append(buf, fmt.Sprintf("(%s)", strings.Join(line, ",")))
			if len(buf) < bulksize {
				continue
			}
			payload := &api.RPCRequest{
				Stmt: fmt.Sprintf("INSERT INTO %s VALUES %s",
					info.Name,
					strings.Join(buf, "")),
			}
			StmtLog(payload.Stmt)
			json.NewEncoder(w).Encode(payload)
			buf = buf[:0]
		}
		if len(buf) > 0 {
			payload := &api.RPCRequest{
				Stmt: fmt.Sprintf("INSERT INTO %s VALUES %s",
					info.Name,
					strings.Join(buf, "")),
			}
			StmtLog(payload.Stmt)
			json.NewEncoder(w).Encode(payload)
			buf = buf[:0]
		}
	}()
	return r
}

func StreamCommands(host, port string, input io.ReadCloser) {
	if !Debug {
		bigobjectURL := url.URL{
			Scheme: "http",
			Host:   net.JoinHostPort(host, port),
			Path:   "cmd/pipe",
		}
		resp, err := http.Post(bigobjectURL.String(), "application/json", input)
		if err != nil {
			log.Fatalln(err)
		}
		defer func() { resp.Body.Close() }()
		io.Copy(ioutil.Discard, resp.Body)
	} else {
		io.Copy(ioutil.Discard, input)
	}
}

func main() {
	StreamCommands(Host, Port, BuildRequestFromCSV(FPath, &Info, BulkSize))
}
