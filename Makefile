.PHONY: dev build clean install publish fmt

NVM_DIR := $(HOME)/.nvm
SHELL := /bin/bash
.SHELLFLAGS := -c 'source $(NVM_DIR)/nvm.sh && eval "$$0" "$$@"'

dev:
	npm run dev

fmt:
	npm run fmt

# Builds single file html
build:
	npm run build
	@echo "Built dist/index.html"
	@cp dist/index.html pipeviz.html
	@echo "Copied to pipeviz.html"

# Preview production build
preview:
	npm run preview

install:
	npm install

clean:
	rm -rf dist node_modules

publish: build
	rsync -avz --delete dist/ root@nargothrond.xyz:/var/www/pipeviz.org/
