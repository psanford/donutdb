package donuthttp

import (
	"bytes"
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client/metadata"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

var nowFunc = time.Now

func (s *Server) verifyRequest(origRequest *http.Request, body []byte) error {
	r := origRequest.Clone(context.Background())

	origSig, err := v4.GetSignedRequestSignature(r)
	if err != nil {
		return err
	}

	creds := credentials.NewStaticCredentials(s.AccessKey, s.SecretAccessKey, "")
	region := s.getRegion()

	bodyReader := bytes.NewReader(body)

	dateStr := r.Header.Get("x-amz-date")
	if dateStr == "" {
		return errors.New("Bad x-amz-date header")
	}

	ts, err := time.Parse(timeFormat, dateStr)
	if err != nil {
		return errors.New("Bad x-amz-date header")
	}

	now := nowFunc()
	delta := now.Sub(ts)
	if delta > 5*time.Minute {
		return errors.New("x-amz-date too old")
	}
	if delta < -5*time.Minute {
		return errors.New("x-amz-date too far into the future")
	}

	authHeader, err := parseAuthHeader(r.Header.Get("authorization"))
	if err != nil {
		return errors.New("Bad authorization header")
	}

	signedHeaders := make(http.Header)
	for _, h := range authHeader.signedHeaders {
		signedHeaders.Set(h, "1")
	}

	// remove non-signed headers from our cloned request
	// some headers must be included in the signature if they are
	// in the request, so don't remove those
	for h := range r.Header {
		canonical := textproto.CanonicalMIMEHeaderKey(h)
		if _, mustSign := mustSignHeaders[canonical]; mustSign {
			continue
		}
		if signedHeaders.Get(canonical) == "" {
			delete(r.Header, h)
		}
	}

	req := request.Request{
		Config: aws.Config{
			Credentials: creds,
			Region:      &region,

			// uncomment for signature logging details
			// LogLevel:    aws.LogLevel(aws.LogDebugWithSigning),
			// Logger:      aws.NewDefaultLogger(),
		},
		ClientInfo: metadata.ClientInfo{
			ServiceName: "dynamodb",
		},

		HTTPRequest: r,
		Body:        bodyReader,
	}

	nowFunc := func() time.Time {
		return ts
	}

	// prime req.safeBody
	req.ResetBody()

	v4.SignSDKRequestWithCurrentTime(&req, nowFunc)

	if req.Error != nil {
		return req.Error
	}

	signer := v4.NewSigner(creds)
	signer.Debug = aws.LogDebugWithSigning
	signer.Logger = aws.NewDefaultLogger()

	calcSig, err := v4.GetSignedRequestSignature(r)
	if err != nil {
		return err
	}

	if subtle.ConstantTimeCompare(origSig, calcSig) == 1 {
		return nil
	}

	return errors.New("Invalid signature")
}

type authHeader struct {
	algorithm  string
	credential struct {
		keyID string
		scope string
	}
	signedHeaders []string
	signature     string
}

func parseAuthHeader(header string) (*authHeader, error) {
	parts := strings.Split(header, " ")
	if len(parts) != 4 {
		return nil, errors.New("Invalid auth header")
	}

	var (
		alg           = parts[0]
		cred          = parts[1]
		signedHeaders = parts[2]
		signature     = parts[3]
	)

	if alg != "AWS4-HMAC-SHA256" {
		return nil, errors.New("Unsupported algorithm")
	}

	var tidyErr error

	tidy := func(s, prefix, suffix string) string {
		if !strings.HasPrefix(s, prefix) {
			tidyErr = errors.New("Invalid auth header")
			return ""
		}

		if !strings.HasSuffix(s, suffix) {
			tidyErr = errors.New("Invalid auth header")
			return ""
		}

		return s[len(prefix) : len(s)-len(suffix)]
	}

	cred = tidy(cred, "Credential=", ",")
	signedHeaders = tidy(signedHeaders, "SignedHeaders=", ",")
	signature = tidy(signature, "Signature=", "")

	if tidyErr != nil {
		return nil, tidyErr
	}

	credParts := strings.SplitN(cred, "/", 2)
	if len(credParts) != 2 {
		return nil, errors.New("Invalid auth header")
	}

	ah := authHeader{
		algorithm: alg,
		credential: struct {
			keyID string
			scope string
		}{
			keyID: credParts[0],
			scope: credParts[1],
		},
		signedHeaders: strings.Split(signedHeaders, ";"),
		signature:     signature,
	}

	return &ah, nil
}

func (s *Server) getRegion() string {
	if s.Region != "" {
		return s.Region
	} else {
		return "us-west-2"
	}
}

var mustSignHeaders = map[string]struct{}{
	"Cache-Control":                         struct{}{},
	"Content-Disposition":                   struct{}{},
	"Content-Encoding":                      struct{}{},
	"Content-Language":                      struct{}{},
	"Content-Md5":                           struct{}{},
	"Content-Type":                          struct{}{},
	"Expires":                               struct{}{},
	"If-Match":                              struct{}{},
	"If-Modified-Since":                     struct{}{},
	"If-None-Match":                         struct{}{},
	"If-Unmodified-Since":                   struct{}{},
	"Range":                                 struct{}{},
	"X-Amz-Acl":                             struct{}{},
	"X-Amz-Copy-Source":                     struct{}{},
	"X-Amz-Copy-Source-If-Match":            struct{}{},
	"X-Amz-Copy-Source-If-Modified-Since":   struct{}{},
	"X-Amz-Copy-Source-If-None-Match":       struct{}{},
	"X-Amz-Copy-Source-If-Unmodified-Since": struct{}{},
	"X-Amz-Copy-Source-Range":               struct{}{},
	"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": struct{}{},
	"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       struct{}{},
	"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   struct{}{},
	"X-Amz-Grant-Full-control":                                    struct{}{},
	"X-Amz-Grant-Read":                                            struct{}{},
	"X-Amz-Grant-Read-Acp":                                        struct{}{},
	"X-Amz-Grant-Write":                                           struct{}{},
	"X-Amz-Grant-Write-Acp":                                       struct{}{},
	"X-Amz-Metadata-Directive":                                    struct{}{},
	"X-Amz-Mfa":                                                   struct{}{},
	"X-Amz-Request-Payer":                                         struct{}{},
	"X-Amz-Server-Side-Encryption":                                struct{}{},
	"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id":                 struct{}{},
	"X-Amz-Server-Side-Encryption-Customer-Algorithm":             struct{}{},
	"X-Amz-Server-Side-Encryption-Customer-Key":                   struct{}{},
	"X-Amz-Server-Side-Encryption-Customer-Key-Md5":               struct{}{},
	"X-Amz-Storage-Class":                                         struct{}{},
	"X-Amz-Tagging":                                               struct{}{},
	"X-Amz-Website-Redirect-Location":                             struct{}{},
	"X-Amz-Content-Sha256":                                        struct{}{},
}
