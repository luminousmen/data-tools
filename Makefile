
.PHONY: init
init:
	poetry install
	poetry env info

.PHONY: test
test: init
	poetry run pytest --tb=short tests/

.PHONY: lint
lint: init
	poetry run flake8 src/data_tools tests/
	poetry run mypy src/data_tools

.PHONY: clean
clean:
	# test artifacts
	rm -rf .coverage coverage.xml report.xml .pytest_cache htmlcov
	# build artifacts
	rm -rf requirements.txt .eggs build dist src/data_tools.egg-info

.PHONY: coverage
coverage: init
	poetry run py.test \
		--verbose \
		--cov-report term-missing \
		--cov-report html \
		--cov=data_tools \
		tests/