.PHONY: skeleton smoke figures audit review-bundle scrna visium validate app q1-bench

ARGS ?=

skeleton:
	python3 scripts/pipeline/run.py skeleton

smoke:
	python3 scripts/pipeline/run.py smoke $(ARGS)

figures:
	python3 scripts/pipeline/run.py figures $(ARGS)

audit:
	python3 scripts/pipeline/run.py audit $(ARGS)

review-bundle:
	python3 scripts/pipeline/run.py review-bundle $(ARGS)

scrna:
	python3 scripts/pipeline/run.py scrna $(ARGS)

visium:
	python3 scripts/pipeline/run.py visium $(ARGS)

validate:
	python3 scripts/pipeline/validate_contract.py $(ARGS)

app:
	python3 -m venv .venv_app
	. .venv_app/bin/activate && pip install --no-cache-dir -r requirements/app.txt
	. .venv_app/bin/activate && uvicorn app.main:app --host $${CBA_HOST:-127.0.0.1} --port $${CBA_PORT:-8000}

q1-bench:
	bash scripts/cloud/run_q1_benchmark_suite.sh $(ARGS)
