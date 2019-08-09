#from utils.rabbitmq import sendEvent
from pyunpack import Archive
import os

# def want(message):
#     return False

# def run(message):
#     sendEvent(message)


if __name__ == "__main__":
    os.makedirs('tmp', exist_ok = True)
    Archive('test-tull.iso').extractall('tmp')
