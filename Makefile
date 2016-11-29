GO := GO15VENDOREXPERIMENT="1" go

.PHONY: build importer syncer checker loader test check deps

build: importer syncer checker loader check test

importer:
	$(GO) build -o bin/importer ./importer

syncer:
	$(GO) build -o bin/syncer ./syncer

checker:
	$(GO) build -o bin/checker ./checker

loader:
	$(GO) build -o bin/loader ./loader

test:

check:
	$(GO) get github.com/golang/lint/golint

	$(GO) tool vet . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	$(GO) tool vet --shadow . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	golint ./... 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	gofmt -s -l . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
