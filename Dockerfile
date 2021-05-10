FROM apache/beam_python3.7_sdk

WORKDIR /pipelines
COPY pipelines/ .
COPY requirements.txt .
RUN pip install -r requirements.txt