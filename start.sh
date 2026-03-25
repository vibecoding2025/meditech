#!/bin/bash
cd "$(dirname "$0")"
echo "Installing dependencies..."
pip3 install -r requirements.txt --quiet
echo ""
echo "Starting Meditech CSV Processor..."
python3 app.py
