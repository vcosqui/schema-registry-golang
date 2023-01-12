#How to run

set Schema registry credentials as env vars
```shell
export SR_USER="xxx"
export SR_PASSWORD="xxx"
```

set broker credentials as env vars
```shell
export SASL_USERNAME="xxx"
export SASL_PASSWORD="xxx"
```

compile and run
```shell
go build && go run producer.go
```
