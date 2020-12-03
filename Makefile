SHELL=/bin/bash

.DEFAULT_GOAL = run

.PHONY: run
run:
	./container-run

.PHONY: container
container:
	./container-build


.PHONY: test
test:
	export PYTHONDONTWRITEBYTECODE=1 && time pytest -n8 -p no:cacheprovider -o console_output_style=classic

.PHONY: pylint
pylint:
	export PYTHONDONTWRITEBYTECODE=1 && time pytest -n8 -p no:cacheprovider -o console_output_style=classic test_sfauto/test_20_pylint.py

.PHONY: clean
clean:
	./container-delete
	find . -name "*.pyc" -exec rm -f {} \;
