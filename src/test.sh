#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

# Coverage 6: coverage run --data-file=/tmp/.coveragerc …
export COVERAGE_FILE=/tmp/.coverage

echo "Running unit tests"
coverage run --source=./gobdistribute -m pytest tests/

echo "Coverage report"
coverage report --show-missing --fail-under=100

echo "Running style checks"
flake8
