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
clientID = None

def UILoop():
    while True:
        numbers = requestNumbers()
        if numbers == "EXIT":
            break
        elif numbers:
            sendWork(numbers)

def requestNumbers():
    userInput = input("Give a list of integers separated by space: ")
    intList = userInput.split(" ")
    if(len(intList) == 0):
        print("No input")
        return "EXIT"
    try:
        intList = [int(a) for a in intList]
    except:
        print("Not all inputs were int")
        return "EXIT"
    print(intList)
    for i in range(len(intList)):
        break
        if(type(intList[i]) == int):
            if(intList[i] <= 0):
                break
            else:
                pass
        else:
            print("Incorrect input")
            return "EXIT"
    else:
        print("Not all numbers were postive")
        return "EXIT"
    return intList

def threadStart(function, arguments=None):
    cThread = threading.Thread(target=function)
    cThread.daemon = True
    cThread.start()

def sendWork(numbers):
    global clientID
    send({"user": "client", "id": clientID, "cmd": "work", "workLoad": numbers})

def send(jsonAbleString):
    global s
    s.send(json.dumps(jsonAbleString).encode())

def pause():
    time.sleep(2)
    print("break")

def startTCPListener(brokerIP=brokerIP, port=PORT):
    global s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((brokerIP, PORT))
    join(s)
    while True:
        try:
            data = s.recv(1024).decode()
            print("received", data)
            print(data)
            data = json.loads(data)
            try:
                if data["cmd"] == "result":
                    print(data)
                elif data["cmd"] == "workAccept":
                    print(data)
                elif data["cmd"] == "ping":
                    send("pong")
                    print(data)
            except KeyError:
                continue
        except KeyboardInterrupt:
            s.close()
            break
        except Exception as e:
            print("TCPLISTENER CRASHED")
            print(e)
            s.close()

def readID(fileName="clientID"):
    with open(fileName, "r") as f:
        temp = f.read().strip()
        if(temp == ""):
            temp = None
        return temp

def writeID(clientID, fileName="clientID"):
    f = open(fileName, "w")
    f.write(str(clientID))

def getClientID(brokerIP=brokerIP, port=PORT):
    global clientID
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((brokerIP, PORT))
        s.send(json.dumps({"user": "client", "cmd": "join", "id": readID()}).encode())
        data = s.recv(1024).decode().strip()
        clientID = json.loads(data)["id"]
        writeID(clientID)

def join(s):
    s.send(json.dumps({"user": "client", "cmd": "join", "id": clientID}).encode())

def main():
    getClientID()
    threadStart(startTCPListener)
    time.sleep(1)
    UILoop()

if __name__ == "__main__":
    main()
