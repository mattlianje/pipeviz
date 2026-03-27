.PHONY: dev build clean install preview fmt

SHELL := /bin/bash
NVM = source $(HOME)/.nvm/nvm.sh; nvm use --silent &&

dev:
	$(NVM) npm run dev

fmt:
	$(NVM) npm run fmt

build:
	$(NVM) npm run build
	@echo "Built dist/index.html"
	@cp dist/index.html pipeviz.html
	@echo "Copied to pipeviz.html"

preview:
	$(NVM) npm run preview

install:
	$(NVM) npm install

clean:
	rm -rf dist node_modules
