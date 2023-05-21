
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
	rm -rf .coverage coverage.xml report.xml .pytest_cache htmlcov
	rm -rf .eggs build dist src/data_tools.egg-info

.PHONY: coverage
coverage: init
	poetry run py.test \
		--verbose \
		--cov-report term-missing \
		--cov-report html \
		--cov=data_tools \
		tests/