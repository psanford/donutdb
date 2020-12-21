package donuthttp

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/private/protocol/json/jsonutil"
	"github.com/felixge/httpsnoop"
	"github.com/psanford/donutdb"
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
	LogLevel            logger.LogLevelType

	DB *donutdb.DonutDB
}

func NewServer(db *donutdb.DonutDB) *Server {
	return &Server{
		Listener: newLocalListener(),
		DB:       db,
		LogLevel: logger.LogHTTPRequests,
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
	mux.HandleFunc("/", s.loggingMiddleware(s.handleRequest))

	http.Serve(s.Listener, mux)
}

func (s *Server) loggingMiddleware(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Logger == nil {
			handler(w, r)
			return
		}

		if s.LogLevel&logger.LogHTTPRequests == 0 {
			handler(w, r)
			return
		}

		url := *r.URL
		host := r.Host
		remoteAddr := r.RemoteAddr
		target := r.Header.Get("X-Amz-Target")

		var metrics httpsnoop.Metrics
		defer func() {
			if s.LogLevel&logger.LogHTTPRequests == logger.LogHTTPRequests {
				logger.LogFields(s.Logger,
					"evt", "http_request",
					"host", host, "url", url.String(), "target", target, "remote_addr", remoteAddr,
					"status_code", metrics.Code, "duration_ms", metrics.Duration.Milliseconds(),
					"bytes_written", metrics.Written,
				)
			}
		}()

		metrics = httpsnoop.CaptureMetrics(handler, w, r)
	}

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
				s.Logger.Log("evt", "invalid_request_err", "err", err)
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

	result, err := s.DB.Dispatch(method, body)
	if err != nil {
		if dbErr, ok := err.(donutdb.HTTPError); ok {
			out, err := json.Marshal(dbErr)
			if err != nil {
				http.Error(w, "Marshal response err", http.StatusInternalServerError)
				return
			}
			http.Error(w, string(out), dbErr.HTTPCode())
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	outJson, err := jsonutil.BuildJSON(result)
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
