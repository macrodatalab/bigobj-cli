package main

import (
	"github.com/gorilla/websocket"
	"github.com/macrodatalab/bigobj-cli/util/api"

	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

var (
	// location of our bigobject service daemon
	host  string
	port  string
	quiet bool
	file  string

	// flag for whether to get the resource handle
	handle bool

	// flag for whether to report elapsed time
	timed bool
)

const (
	defaultHost   = "localhost"
	defaultPort   = "9090"
	defaultQuiet  = false
	defaultHandle = false
	defaultTimed  = false

	GET_RESULT_NOW = 0
	NOOP           = 1
	CONFIG         = 2
	FETCH_DATA     = 3
)

func init() {
	flag.StringVar(&host, "H", defaultHost, "Provide hostname to bigobject")
	flag.StringVar(&port, "P", defaultPort, "Provide port to bigobject")
	flag.BoolVar(&quiet, "S", defaultQuiet, "Stay quiet and only show results")
	flag.BoolVar(&handle, "handle", defaultHandle, "Whether to obtain handle first")
	flag.BoolVar(&timed, "time", defaultTimed, "Whether to report exec time")
	flag.StringVar(&file, "F", "", "Provide Lua script to load")
	flag.Parse()
}

func MakeReadline() (chan string, chan time.Duration) {
	text, next := make(chan string), make(chan time.Duration)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				if !quiet {
					log.Println(err)
				}
			}
			os.Exit(0)
		}()
		input := bufio.NewScanner(os.Stdin)
		for cost := range next {
		prompt:
			if timed && cost > 0 {
				fmt.Println("Operation time:", cost)
			}
			if !quiet {
				fmt.Print("bosh>> ")
			}
			if input.Scan() {
				line := input.Text()
				if len(line) == 0 {
					goto prompt
				} else {
					text <- line
				}
			} else {
				panic(input.Err())
			}
		}
	}()
	next <- time.Duration(0)
	return text, next
}

func parseAction(stmt string) (action int) {
	tokens := strings.Fields(stmt)
	if len(tokens) < 1 {
		log.Fatalln("Unable to continue:", stmt)
	}
	switch strings.ToUpper(tokens[0]) {
	default:
		action = NOOP
		break
	case "SELECT":
	case "FIND":
	case "GET":
		action = GET_RESULT_NOW
		break
	case "SCAN":
		action = FETCH_DATA
		break
	}
	return
}

func CmdLoop() {
	const (
		// Time allowed to write a message to the peer.
		writeWait = 10 * time.Second

		// Time allowed to resond to a ping event from peer
		pingWait = 55 * time.Second
	)

	var (
		// Connection object
		ws *websocket.Conn
	)

	bigobjectURL := url.URL{
		Scheme: "ws",
		Host:   net.JoinHostPort(host, port),
		Path:   "exec",
	}

	var err error
	dialer := websocket.DefaultDialer
	if ws, _, err = dialer.Dial(bigobjectURL.String(), nil); err != nil {
		if !quiet {
			log.Fatalln(err)
		} else {
			os.Exit(1)
		}
	}
	ws.SetPingHandler(nil)

	input, next := MakeReadline()
	ticker := time.Tick(pingWait)

	defer func() {
		if err := recover(); err != nil {
			ws.Close()
			if !quiet {
				log.Fatalln(err)
			} else {
				os.Exit(1)
			}
		}
	}()

	for {
		select {
		case stmt, ok := <-input:
			if !ok {
				return
			}
			now := time.Now()
			switch parseAction(stmt) {
			default:
				req := &api.RPCRequest{Stmt: stmt}
				if err := ws.WriteJSON(req); err != nil {
					if !quiet {
						fmt.Println(err)
					}
				} else {
					var resp interface{}
					if err := ws.ReadJSON(&resp); err != nil {
						panic(err)
					}
					text, _ := json.MarshalIndent(resp, "", "    ")
					fmt.Println(string(text))
				}
				break
			case GET_RESULT_NOW:
				req := &api.RPCRequest{Stmt: stmt, Opts: &api.RPCOpts{Handle: handle}}
				if err := ws.WriteJSON(req); err != nil {
					if !quiet {
						fmt.Println(err)
					}
				} else if !handle {
					var idx int64 = 1
					for idx > 0 {
						var resp map[string]interface{}
						if err := ws.ReadJSON(&resp); err != nil {
							panic(err)
						}
						if thing, ok := resp["Content"]; ok && thing != nil {
							payload := thing.(map[string]interface{})
							data := payload["content"].([]interface{})
							idx = int64(payload["index"].(float64))
							for _, thing := range data {
								text, _ := json.MarshalIndent(thing, "", "    ")
								fmt.Println(string(text))
							}
						} else {
							text, _ := json.MarshalIndent(resp, "", "    ")
							fmt.Println(string(text))
							break
						}
					}
				} else {
					var resp interface{}
					if err := ws.ReadJSON(&resp); err != nil {
						panic(err)
					}
					text, _ := json.MarshalIndent(resp, "", "    ")
					fmt.Println(string(text))
				}
				break
			case FETCH_DATA:
				req := &api.RPCRequest{Stmt: stmt}
				if err := ws.WriteJSON(req); err != nil {
					if !quiet {
						fmt.Println(err)
					}
				} else {
					var idx int64 = 1
					for idx > 0 {
						var resp map[string]interface{}
						if err := ws.ReadJSON(&resp); err != nil {
							panic(err)
						}
						if thing, ok := resp["Content"]; ok && thing != nil {
							payload := thing.(map[string]interface{})
							data := payload["content"].([]interface{})
							idx = int64(payload["index"].(float64))
							for _, thing := range data {
								text, _ := json.MarshalIndent(thing, "", "    ")
								fmt.Println(string(text))
							}
						} else {
							text, _ := json.MarshalIndent(resp, "", "    ")
							fmt.Println(string(text))
							break
						}
					}
				}
			}
			next <- time.Since(now)
		case <-ticker:
			ws.WriteControl(
				websocket.PongMessage,
				[]byte{},
				time.Now().Add(writeWait),
			)
		}
	}
}

func OneLoadScript() {
	var (
		// sourece File object to process
		source io.Reader

		// type of data being transfered
		contentType string
	)

	if src, err := os.Open(file); err != nil {
		if !quiet {
			log.Fatalln(err)
		} else {
			os.Exit(1)
		}
	} else {
		source = src
	}

	if path.Ext(file) == ".gz" {
		contentType = "application/gzip"
	} else {
		contentType = "text/plain"
	}

	bigobjectURL := url.URL{
		Scheme: "http",
		Host:   fmt.Sprint(host, ":", port),
		Path:   "script/" + path.Base(file),
	}

	if resp, err := http.Post(bigobjectURL.String(), contentType, source); err != nil {
		if !quiet {
			log.Fatalln(err)
		} else {
			os.Exit(1)
		}
	} else {
		if data, err := ioutil.ReadAll(resp.Body); err != nil {
			log.Fatalln(err)
		} else {
			log.Println(string(data))
		}
	}
}

func main() {
	if file == "" {
		CmdLoop()
	} else {
		OneLoadScript()
	}
}
