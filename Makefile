.PHONY: install_dev
install_dev:
	python -m pip install ".[dev]/"

.PHONY: install_test
install_test:
	python -m pip install ".[test]/"

.PHONY: run_test
run_test:
	pytest .\tests\test.py
