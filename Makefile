.PHONY: dev build clean install publish fmt

SHELL := /bin/bash
NVM = source $(HOME)/.nvm/nvm.sh; nvm use --silent &&

dev:
	$(NVM) npm run dev

fmt:
	$(NVM) npm run fmt

# Builds single file html
build:
	$(NVM) npm run build
	@echo "Built dist/index.html"
	@cp dist/index.html pipeviz.html
	@echo "Copied to pipeviz.html"

# Preview production build
preview:
	$(NVM) npm run preview

install:
	$(NVM) npm install

clean:
	rm -rf dist node_modules

publish: build
	rsync -avz --delete dist/ root@nargothrond.xyz:/var/www/pipeviz.org/
