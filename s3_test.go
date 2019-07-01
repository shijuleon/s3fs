package s3fs

import (
	"log"
	"testing"
)

type testCase struct {
	fileName     string
	fileSize     int64
	fileReadSize int
	description  string
}

var testCases = []testCase{
	testCase{
		fileName:     "passengers.txt",
		fileSize:     1046,
		fileReadSize: 1046,
		description:  "A small file of 1KB",
	},
	testCase{
		fileName:     "test_dataset_large.json",
		fileSize:     102,
		fileReadSize: 2049,
		description:  "A file of size approx 300MB",
	},
	testCase{
		fileName:     "wikipedia-20150518.bin",
		fileSize:     1032,
		fileReadSize: 1024,
		description:  "A file of size approx 20GB",
	},
}

func TestNew(t *testing.T) {
	New("public-sample-data", "us-east-1")
}

func TestOpen(t *testing.T) {
	s3Fs := New("public-sample-data", "us-east-1")
	f, err := s3Fs.Open("passengers.txt")
	if err != nil {
		log.Fatalf("Error opening passengers.txt: %s", err)
	}

	stat, _ := f.Stat()
	if stat.Size() != 1046 {
		log.Fatalf("error: size doesn't match")
	}
}
func TestFileOpen(t *testing.T) {
	s3Fs := New("public-sample-data", "us-east-1")

	for i, t := range testCases {
		log.Printf("%d. %s", i+1, t.description)
		f, err := s3Fs.Open(t.fileName)
		if err != nil {
			log.Fatalf("error: opening %s: %s", t.fileName, err)
		}

		p := make([]byte, t.fileReadSize)
		n, err := f.Read(p)
		if err != nil {
			log.Fatalf("error: reading file: %s", err)
		}

		if n != t.fileReadSize {
			log.Fatalf("error: size doesn't match")
		}
	}
}

func TestFileStat(t *testing.T) {
	s3Fs := New("public-sample-data", "us-east-1")

	for _, t := range testCases {
		f, err := s3Fs.Open(t.fileName)
		if err != nil {
			log.Fatalf("error: opening %s: %s", t.fileName, err)
		}

		stat, _ := f.Stat()
		if stat.Size() != t.fileSize {
			log.Fatalf("error: size doesn't match")
		}

		if stat.Name() != t.fileName {
			log.Fatalf("error: name doesn't match")
		}
	}
}
