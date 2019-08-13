# violent-python-processing

## Getting started

### Requirements
* Python 3
* Rabbitmq
* Docker (optional, but recommended)

#### Setting up rabbitmq
```bash
# Starting rabbitmq for local dev
docker run -d --network="host" --name rabbitmq rabbitmq:latest
```

#### Setting up python for local dev
```bash
cd <project-root>
# Setup python virutalenv
python3 -m venv venv
# Activate virtualenv
. venv/bin/activate
# Installing deps
pip install -r requirements.txt
# Create input, output, processing folders
mkdir input output processing
# Starting application
python main.py
```

For full extraction capabilities, extra system packages are needed.
```bash
sudo apt install unzip unrar p7zip-full default-jre
```

#### Supported input files
The files need to be NIFI flow file streams

#### Usage
Drop NIFI flow file streams in `input`folder. Processing output will be placed in `output`folder

## Project structure

Every py file in processors will be automatically imported.

Requirements for a processors is 2 functions:
* get_mime_types()
* run(message)


Process manager controls the processing flow:
* Watch input directory and unpack flow file stream
* Handle events to rabbitmq
* Startup processing for new events
* Output flow file stream to output directory

Rabbitmq is just used for a simple queue for processing tasks.