FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      default-jdk \
      build-essential \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

RUN export PYTHONPATH="$PYTHONPATH:$PWD"

