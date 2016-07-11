FROM golang:1.5

ENV GO15VENDOREXPERIMENT=1

RUN mkdir -p /go/src/github.com/pagarme/teleport/
WORKDIR /go/src/github.com/pagarme/teleport/

ADD . /go/src/github.com/pagarme/teleport/

RUN go get -u github.com/jteeuwen/go-bindata/...

RUN make install
