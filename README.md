Environment Variables
---------------------

* `KAFKA_REST_PROXY_URL`: URL for Kafka REST Proxy
* `MAX_RETRIES`: Maximum amount of retries before failing

Usage
-----

Virtual environment recommended. Assumes REST Proxy is running locally with defaults.

    pip install -r requirements.txt
    export KAFKA_REST_PROXY_URL=http://localhost:8082
    export MAX_RETRIES=10
    ./main.py
