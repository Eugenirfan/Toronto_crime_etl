FROM prefecthq/prefect:2.7.7-python3.11

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

WORKDIR  /data

COPY ingest_data.py ingest_data.py
ENTRYPOINT [ "python", "ingest_data.py" ]