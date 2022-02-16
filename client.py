import time
import socket
import json
import threading

"""
json:
{"cmd": ""}

"""
global s
brokerIP="127.0.0.1"
PORT = 25566
clientID = 1

def UILoop():
    while True:
        numbers = requestNumbers()
        if numbers == "EXIT":
            break
        elif numbers:
            sendWork(numbers)

def requestNumbers():
    userInput = input("Give a list of integers separated by space: ")
    intList = userInput.split()
    if(len(intList) == 0):
        print("Incorrect input")

    for i in range len(intList):
        if(type(intList[i]) == int):
                if(intList[i] > 0):
                    return intList:
                else:
                    print("Incorrect input")
                    return "EXIT":
            else:
                print("Incorrect input")
                return "EXIT":

def threadGo(function, arguments=None):
    cThread = threading.Thread(target=function)
    cThread.daemon = True
    cThread.start()

def sendWork(numbers):
    global clientID
    send({"user": "client", "id": clientID, "cmd": "work", "workLoad": numbers}

def send(jsonAbleString):
    global s
    s.send(json.dumps(jsonAbleString).encode())

def startTCPListener(brokerIP=brokerIP, port=PORT):
    global s
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((brokerIP, PORT))
                while True:
                    data = s.recv(1024)
                    print("received", data.decode())
                    data = json.load(data)
                    if data["cmd"] == "result":
                        print(data)
                    if data["cmd"] == "workAccept":
                        print(data)
                    if data["cmd"] == "ping":
                        send("pong")
                        print(data)
        except KeyboardInterrupt:
            s.close()
            break
        except Exception as e:
            print(e)
            s.close()

def readID(fileName="clientID"):
    f = open(fileName, "r")
    temp = f.read().strip()
    if(temp == "")
        temp = getClientID()
    return temp

def writeID(clientID, fileName="clientID"):
    f = open(fileName, "w")
    f.write(string(clientID))

def getClientID(brokerIP=brokerIP, port=PORT):
    global clientID
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((brokerIP, PORT))
        s.send(json.dumps({"user": "client", "cmd": "join", "id": readID()}).encode())
        data = s.recv(1024).decode().strip()
        clientID = json.loads(data)["id"]

def main():
    getClientID()
    startTCPListener()
    UILoop()

if __name__ == "__main__":
