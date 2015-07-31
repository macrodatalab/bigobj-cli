package api

type RPCOpts struct {
	Handle bool `json:,omitempty`
}

type RPCRequest struct {
	Stmt      string
	Workspace string
	Opts      *RPCOpts `json:,omitempty`
}

type RawMessage string

func (m RawMessage) MarshalJSON() ([]byte, error) {
	return []byte(m), nil
}

type RPCResponse struct {
	Content RawMessage
	Status  int
	Err     string
}
