#!/bin/bash

source venv/scripts/activate

export PYTHONPATH=$(pwd)

set -a
source .env.mongodb
set +a
echo "env variables loaded from .env.mongodb"