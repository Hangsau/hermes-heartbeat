.PHONY: test test-cov lint clean

test:
	python3 -m pytest test_heartbeat_v2.py -v

test-cov:
	python3 -m pytest test_heartbeat_v2.py -v --cov=heartbeat_v2 --cov-report=term-missing

lint:
	python3 -m py_compile heartbeat_v2.py test_heartbeat_v2.py

clean:
	rm -rf __pycache__ .pytest_cache .coverage
