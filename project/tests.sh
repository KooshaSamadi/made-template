
#!/bin/bash

set -e

echo "Running..."

pytest project/tests_pipeline.py
# I also enable Dependabot for automatically checks for and suggests updates to the dependencies used in your project. 
