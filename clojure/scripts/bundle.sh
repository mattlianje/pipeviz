#!/bin/bash
# Bundles compiled JS into a single HTML file

set -e

cd "$(dirname "$0")/.."

# Read files
HTML_TEMPLATE=$(cat resources/public/index.html)
JS_CONTENT=$(cat dist/js/main.js)

# Create bundled HTML by replacing script tag with inline JS
mkdir -p dist

# Use awk to replace the script tag
awk -v js="$JS_CONTENT" '
/<script src="js\/main.js"><\/script>/ {
    print "<script>"
    print js
    print "</script>"
    next
}
{ print }
' resources/public/index.html > dist/pipeviz.html

SIZE=$(du -h dist/pipeviz.html | cut -f1)
echo "Bundled to dist/pipeviz.html ($SIZE)"
