FROM golang:latest

ADD https://github.com/golang/dep/releases/download/v0.4.1/dep-linux-amd64 /usr/bin/dep
RUN chmod +x /usr/bin/dep

WORKDIR $GOPATH/src/github.com/DigWing/mapreduce

COPY Gopkg.toml Gopkg.lock ./
RUN dep ensure --vendor-only

COPY . ./

EXPOSE 8080

CMD ["go","run","main.go"]