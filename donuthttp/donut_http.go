package donuthttp

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/psanford/donutdb/api"
	"github.com/psanford/donutdb/logger"
)

const (
	maxBodySize = 1 << 20
	timeFormat  = "20060102T150405Z"
)

type Server struct {
	URL      string // base URL of form http://ipaddr:port with no trailing slash
	Listener net.Listener

	// sqlite connection string.
	// Defaults to an in memory sqlite database.
	DBSourceName string

	AccessKey           string
	SecretAccessKey     string
	InsecureDisableAuth bool
	Region              string
	Logger              logger.Logger

	db *api.DBState
}

func NewServer() *Server {
	return &Server{
		Listener: newLocalListener(),
	}
}

func (s *Server) Start() {
	if s.URL != "" {
		panic("Server already started")
	}

	s.db = api.New()

	s.URL = "http://" + s.Listener.Addr().String()
	go s.run()
}

func (s *Server) Close() {
	s.Listener.Close()
	// we should wait for clients to drain
}

func (s *Server) run() {

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRequest)

	http.Serve(s.Listener, mux)
}

func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	reader := http.MaxBytesReader(w, r.Body, maxBodySize)
	body, err := ioutil.ReadAll(reader)
	if err != nil {
		return
	}

	if !s.InsecureDisableAuth {
		err := s.verifyRequest(r, body)
		if err != nil {
			if s.Logger != nil {
				s.Logger.Log("invalid request err:", err)
			}
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	if r.Method != "POST" {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	target := r.Header.Get("X-Amz-Target")
	parts := strings.SplitN(target, ".", 2)
	if len(parts) != 2 {
		http.Error(w, "Invalid x-amz-target", http.StatusBadRequest)
		return
	}

	if !strings.HasPrefix(parts[0], "DynamoDB_") {
		http.Error(w, "Invalid x-amz-target", http.StatusBadRequest)
		return
	}

	method := parts[1]

	result, err := s.db.Dispatch(method, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	outJson, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Write(outJson)
}

func newLocalListener() net.Listener {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			panic(fmt.Sprintf("httptest: failed to listen on a port: %v", err))
		}
	}
	return l
}
