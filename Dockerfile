FROM ubuntu:18.04

ENV APP_HOME /opt/app
WORKDIR /opt/app

RUN mkdir -p data/input data/output/ data/processing

RUN apt-get update && apt-get install -y \
    python3-pip \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN pip3 install -r requirements.txt

RUN groupadd -g 1000 ubuntu
RUN useradd \
        -d ${APP_HOME} \
        -u 1000 -g 1000 \
        -s /bin/bash \
        ubuntu

RUN chown -R ubuntu:ubuntu ${APP_HOME}

USER ubuntu

CMD ["python3", "main.py"]
