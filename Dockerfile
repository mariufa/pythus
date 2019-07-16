FROM ubuntu:18.04

WORKDIR /opt/app

RUN mkdir -p data/input data/output/ data/processing

RUN apt-get update && apt-get install -y \
    python3-pip \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip3 install -r requirements.txt

CMD ["python3", "main.py"]
