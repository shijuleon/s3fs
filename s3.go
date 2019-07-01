// Package s3fs provides Go file-like abstractions for S3
package s3fs

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// FileRanges wraps start and end. See FileSystemWithRanges
type FileRanges struct {
	start, end int64
}

// FileSystem implements http.FileSystem
type FileSystem struct {
	s3     *s3.S3
	bucket string
}

// FileSystemWithRanges implements http.FileSystem and supports range requests
// You will need to create a separate FileSystemWithRanges for every request if you are using
// something like http.FileServer. Each request will need to call Open() for its range specified
// in FileSystemWithRanges.ranges.
type FileSystemWithRanges struct {
	s3     *s3.S3
	bucket string
	ranges FileRanges
}

// File implements http.File
type File struct {
	fs   FileSystem
	body io.ReadCloser
	stat fileStat
}

type fileStat struct {
	name    string
	size    int64
	modTime time.Time
}

// New creates FileSystem and doesn't support ranges.
func New(bucket, region string) *FileSystem {
	return &FileSystem{
		s3: s3.New(session.New(), &aws.Config{
			Region: aws.String(region),
		}),
		bucket: bucket,
	}
}

// NewWithRange creates FileSystemWithRanges with support for ranges
func NewWithRange(bucket, region string, ranges FileRanges) *FileSystemWithRanges {
	return &FileSystemWithRanges{
		s3: s3.New(session.New(), &aws.Config{
			Region: aws.String(region),
		}),
		bucket: bucket,
		ranges: ranges,
	}
}

// NewFileRanges is used to define the start and end of the file
func NewFileRanges(start, end int64) FileRanges {
	return FileRanges{
		start: start,
		end:   end,
	}
}

func (f FileSystemWithRanges) getSize(name string) int64 {
	input := &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(name),
	}

	object, err := f.s3.GetObject(input)
	if err != nil {
		return 0
	}

	return aws.Int64Value(object.ContentLength)
}

func newFile(stat fileStat, body io.ReadCloser) (*File, error) {
	return &File{
		body: body,
		stat: stat,
	}, nil
}

// Open returns a File with the name of the object
func (f FileSystem) Open(name string) (http.File, error) {
	name = filepath.Base(name)

	input := &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(name),
	}

	object, err := f.s3.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				return nil, os.ErrNotExist
			default:
				return nil, aerr
			}
		} else {
			return nil, aerr
		}
	}

	stat := fileStat{
		name:    name,
		size:    aws.Int64Value(object.ContentLength),
		modTime: aws.TimeValue(object.LastModified),
	}

	fi, err := newFile(stat, object.Body)
	if err != nil {
		return nil, err
	}

	return fi, nil
}

// Open returns a File with the name of the object
func (f FileSystemWithRanges) Open(name string) (http.File, error) {
	name = filepath.Base(name)

	input := &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(name),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", f.ranges.start, f.ranges.end)),
	}

	object, err := f.s3.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchKey:
				return nil, os.ErrNotExist
			default:
				return nil, aerr
			}
		} else {
			return nil, aerr
		}
	}

	stat := fileStat{
		name:    name,
		size:    f.getSize(name),
		modTime: aws.TimeValue(object.LastModified),
	}

	fi, err := newFile(stat, object.Body)
	if err != nil {
		return nil, err
	}

	return fi, nil
}

func (f fileStat) Name() string {
	return f.name
}

func (f fileStat) Size() int64 {
	return f.size
}

func (f fileStat) Mode() os.FileMode {
	// owner: read, write, execute
	// everyone else: only read
	return os.FileMode(0644)
}

func (f fileStat) ModTime() time.Time {
	return f.modTime
}

func (f fileStat) IsDir() bool {
	return false
}

func (f fileStat) Sys() interface{} {
	return nil
}

// Close closes the file
func (f File) Close() error {
	return f.body.Close()
}

func (f File) Read(p []byte) (int, error) {
	return io.ReadFull(f.body, p)
}

// Readdir returns an empty []os.FileInfo
func (f File) Readdir(count int) ([]os.FileInfo, error) {
	return []os.FileInfo{}, nil
}

// Seek is not implemented. Seek needs the entire file to be on disk or memory. See FileSystemWithRanges
func (f File) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

// Stat behaves like os.Stat
func (f File) Stat() (os.FileInfo, error) {
	return f.stat, nil
}
