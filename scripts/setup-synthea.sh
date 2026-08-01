#!/bin/bash

set -e

if [ ! -d "tools/synthea/source/.git" ]; then
    echo "Cloning Synthea..."
    git clone https://github.com/synthetichealth/synthea.git tools/synthea/source
else
    echo "Updating Synthea..."
    cd tools/synthea/source
    git pull
fi

echo "Synthea ready."