.PHONY: install_dev
install_dev:
	python -m pip install ".[dev]/"

.PHONY: install_test
install_test:
	python -m pip install ".[test]/"

.PHONY: test
test:
	pytest .\tests\test.py

.PHONY: build
build:
	python setup.py sdist

.PHONY: upload
upload:
	twine upload dist/*