package donuthttp

import (
	"io/ioutil"
	"net"
	"net/http"

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
	if r.Method != "POST" {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

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

	w.Write([]byte("ok\n"))
}
