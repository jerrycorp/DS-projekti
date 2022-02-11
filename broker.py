import time
import socket
import json
import threading
"""
json:
{"cmd": ""}

""",
"numbers,"
"numbers
brokerIP="127.0.0.1"
PORT = ??


def send(message):
    BUFFER_SIZE = 1024
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((brokerIP, PORT))
    s.send(message)


    s.close()

if __name__ == "__main__":
