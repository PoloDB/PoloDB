PROJECT_NAME:=polodb

.PHONY: all clean test lint install help

all: install

install:  # Install project dependencies
	@echo "Installing dependencies..."
	@poetry install

test:  # Run tests using pytest through Poetry
	@echo "Running tests..."
	@poetry run pytest

lint:  # Lint and check code style with flake8
	@echo "Linting code..."
	@poetry run flake8 ${PROJECT_NAME}


format-black:  # Format code
	@echo "Formating code..."
	@poetry run black ${PROJECT_NAME}

clean:  # Clean up project directory
	@echo "Cleaning up..."
	@poetry env remove python || true
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@rm -rf .pytest_cache

help:  # Display callable targets.
	@egrep '^(.+)\:\ .*#\ .+' ${MAKEFILE_LIST} | sort | awk 'BEGIN {FS = ": .*# "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
