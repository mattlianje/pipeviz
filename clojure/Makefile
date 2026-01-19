.PHONY: dev build clean install repl bundle server server-dev test fmt publish

SHELL := /bin/bash
NVM = source $(HOME)/.nvm/nvm.sh; nvm use --silent &&

dev:
	$(NVM) npx shadow-cljs watch app

server:
	clj -M:server

server-dev:
	clj -M:server 3000 resources/example.json

test:
	clj -M:test

fmt:
	clj -M:fmt

build: install
	$(NVM) npx shadow-cljs release release

bundle: build
	bash scripts/bundle.sh
	@echo "Built dist/pipeviz.html"
	@cp dist/pipeviz.html pipeviz.html
	@echo "Copied to pipeviz.html"

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

publish: bundle
	rsync -avz --delete dist/ root@nargothrond.xyz:/var/www/pipeviz.org/
