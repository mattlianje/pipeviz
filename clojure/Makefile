.PHONY: dev build clean install repl bundle

SHELL := /bin/bash
NVM = source $(HOME)/.nvm/nvm.sh; nvm use --silent &&

dev:
	$(NVM) npx shadow-cljs watch app

build: install
	$(NVM) npx shadow-cljs release release

bundle: build
	bash scripts/bundle.sh
	@cp dist/pipeviz.html ../pipeviz-cljs.html
	@echo "Copied to ../pipeviz-cljs.html"

install: node_modules

node_modules: package.json
	$(NVM) npm install
	@touch node_modules

repl:
	$(NVM) npx shadow-cljs cljs-repl app

clean:
	rm -rf dist node_modules .shadow-cljs

serve:
	$(NVM) npx shadow-cljs server
