.PHONY: dev build clean install repl bundle

SHELL := /bin/bash
NVM = source $(HOME)/.nvm/nvm.sh; nvm use --silent &&

# Development with hot reload
dev:
	$(NVM) npx shadow-cljs watch app

# Build release version
build: install
	$(NVM) npx shadow-cljs release release

# Bundle into single HTML file
bundle: build
	bash scripts/bundle.sh
	@cp dist/pipeviz.html ../pipeviz-cljs.html
	@echo "Copied to ../pipeviz-cljs.html"

# Install dependencies
install: node_modules

node_modules: package.json
	$(NVM) npm install
	@touch node_modules

# Start a REPL
repl:
	$(NVM) npx shadow-cljs cljs-repl app

# Clean build artifacts
clean:
	rm -rf dist node_modules .shadow-cljs

# Watch and serve for development
serve:
	$(NVM) npx shadow-cljs server
