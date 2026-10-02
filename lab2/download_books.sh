#!/usr/bin/env bash
# Downloads the Project Gutenberg books listed in books.csv into ./input
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p input

tail -n +2 books.csv | while IFS=, read -r id name size; do
  out="input/pg${id}.txt"
  if [[ -s "$out" ]]; then
    echo "skip  $out ($name)"
    continue
  fi
  echo "fetch $out ($name, ~$size)"
  curl -sSfL -o "$out" "https://www.gutenberg.org/cache/epub/${id}/pg${id}.txt"
  sleep 2 # Gutenberg throttles bulk downloads
done

du -sh input
