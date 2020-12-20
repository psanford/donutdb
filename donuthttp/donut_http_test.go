package donuthttp

import (
	"bytes"
	"net/http"
	"testing"
)

func TestVerifySignature(t *testing.T) {

	s := Server{
		AccessKey:       "DUMMYIDEXAMPLE",
		SecretAccessKey: "DUMMYEXAMPLEKEY",
	}

	body := []byte("{}")

	r := mkReq(body)
	err := s.verifyRequest(r, body)
	if err != nil {
		t.Fatalf("verifyRequest failed: %s", err)
	}

	s.SecretAccessKey = "poulticed-crybaby"
	r = mkReq(body)
	err = s.verifyRequest(r, body)
	if err == nil {
		t.Fatalf("expected verifyRequest to fail but it didn't")
	}
}

func mkReq(body []byte) *http.Request {
	r, err := http.NewRequest("POST", "/", bytes.NewBuffer(body))
	if err != nil {
		panic(err)
	}

	r.Host = "172.17.0.2:8000"
	r.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
	r.Header.Set("Content-Type", "application/x-amz-json-1.0")
	r.Header.Set("User-Agent", "aws-cli/1.18.69 Python/3.8.5 Linux/5.4.0-56-generic botocore/1.16.19")
	r.Header.Set("X-Amz-Date", "20201220T012411Z")
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=DUMMYIDEXAMPLE/20201220/us-west-2/dynamodb/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=1683863b2ea3ff4e703f0ee0e6a360b388f6c15934a4693e93ab34c60aa9f18b")
	r.Header.Set("content-length", "2")

	return r
}
