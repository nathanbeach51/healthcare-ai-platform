#!/bin/bash

set -e

echo "Generating synthetic hospital..."

cd tools/synthea/source

./run_synthea \
  -p 100

echo "Synthea generation complete."