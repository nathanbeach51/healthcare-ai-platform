#!/bin/bash

set -e

echo "Generating synthetic hospital..."

mkdir -p tools/synthea/output

docker run --rm \
  -v "$(pwd)/tools/synthea/output:/output" \
  synthetichealth/synthea \
  -p 100

echo "Done!"