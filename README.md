# s3fs
File-like abstractions in Go for AWS S3

https://godoc.org/github.com/shijuleon/s3fs

```go
func (f FileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-Type", "application/octet-stream")
  http.FileServer(s3fs.NewWithRange("public-data", "us-east-1", s3fs.NewFileRanges(1024, 2048))).ServeHTTP(w, r)
}
```
