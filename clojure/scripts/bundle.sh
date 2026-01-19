#!/bin/bash

set -e

cd "$(dirname "$0")/.."

mkdir -p dist

# Get line count of HTML template
TOTAL_LINES=$(wc -l < resources/public/index.html)

# Find the line with the script tag
SCRIPT_LINE=$(grep -n 'script src="js/main.js"' resources/public/index.html | cut -d: -f1)

# Build the bundled html
{
    head -n $((SCRIPT_LINE - 1)) resources/public/index.html
    echo '    <script>'
    cat dist/js/main.js
    echo '    </script>'
    tail -n $((TOTAL_LINES - SCRIPT_LINE)) resources/public/index.html
} > dist/pipeviz.html

SIZE=$(du -h dist/pipeviz.html | cut -f1)
echo "Bundled to dist/pipeviz.html ($SIZE)"
