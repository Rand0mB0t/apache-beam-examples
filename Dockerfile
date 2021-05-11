FROM amd64/python:3.8-slim

WORKDIR /pipelines
COPY pipelines/ .
COPY requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "sample_pipeline_1.py" ]