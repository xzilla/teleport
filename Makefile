all: teleport

asset:
	mkdir -p asset
	$(GOPATH)/bin/go-bindata $(ASSET_FLAGS) -pkg asset -o asset/asset.go data/...

teleport: asset
	go build

install: teleport
	go install

clean:
	rm -rf teleport asset

.PHONY: all clean install teleport asset
