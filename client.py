import time
import socket
import json
import threading
import random
from dataclasses import dataclass
from typing import List
import traceback

"""
json:
{"cmd": ""}

"""
global s
brokerIP="127.0.0.1"
PORT = 25566
clientID = None
results = []
worklist = []
maxInt = 15000000
## TODO: fix sending singular lists
"""
Select if you want to view results: v, add numbers to be factored: a or exit: ev
Give a list of integers separated by space: 123
[123]
Traceback (most recent call last):
  File "G:\Koulu\Distributed systems\client.py", line 161, in <module>
    main()
  File "G:\Koulu\Distributed systems\client.py", line 158, in main
    UILoop()
  File "G:\Koulu\Distributed systems\client.py", line 26, in UILoop
    sendWork(numbers)
  File "G:\Koulu\Distributed systems\client.py", line 86, in sendWork
    send({"user": "client", "id": clientID, "cmd": "work", "workLoad": numbers})
  File "G:\Koulu\Distributed systems\client.py", line 90, in send
    s.send(json.dumps(jsonAbleString).encode())
ConnectionResetError: [WinError 10054] An existing connection was forcibly closed by the remote host
"""

## TODO: make random number generators generate unique numbers
@dataclass
class Work:#(numbers, time):
    numbers: List[int]
    time: int

def checkWorkList(result):
    print("checkworklist")
    global worklist
    numbers = [int(a) for a in list(result.keys())]
    removable = []

    print(worklist)
    for work in worklist:
        #print(work)
        #print(numbers)
        for num in numbers:
            #print(work)
            #print(work.numbers)
            #print(num)
            if num not in work.numbers:
                continue
            work.numbers.remove(num)
            if len(work.numbers) < 1:
                removable.append(work)
                print("Work returned in {0:.2f} s".format(time.time() - work.time))

    worklist = [x for x in worklist if x not in removable]
    print(str(len(worklist)) + " items in work list")


def randomInputGenerator(maxInt=maxInt, amount=20):
    minInt = maxInt/10
    input = []
    for i in range(amount):
        input.append(random.randint(minInt, maxInt))
    return input

def UILoop():
    #Simple console ui
    while True:
        decision = uiDecision()
        if decision == "work":
            numbers = requestNumbers()
            if numbers == "EXIT":
                break
            elif numbers:
                sendWork(numbers)
        elif decision == "results":
            printResults()
        elif decision == "EXIT":
            break
        elif decision =="random":
            sendWork(randomInputGenerator())
        elif decision =="small":
            sendWork(randomInputGenerator(1000,100))


def printResults():
    if(len(results) > 0):
        for n in results:
            print(n)
    else:
        print("No results yet. Try again later.")

def uiDecision():
    print("Hello world!")
    userInput = input("Select if you want to view results: v, add numbers to be factored: a or exit: e")
    if(userInput.lower() == "a"):
        return "work"
    if(userInput.lower() == "v"):
        return "results"
    if(userInput.lower() == "e"):
        return "EXIT"
    if(userInput.lower() == "r"):
        return "random"
    if(userInput.lower() == "s"):
        return "small"

def requestNumbers():
    global maxInt
    userInput = input("Give a list of integers separated by space: (No larger than 15 000 000)")
    if userInput.lower()[0] == "e":
        return "EXIT"
    intList = userInput.split(" ")
    if(len(intList) == 0):
        print("No input")
        return
    try:
        intList = [int(a) for a in intList]
    except:
        print("Not all inputs were int")
        return
    print(intList)
    for i in range(len(intList)):
        break
        if(type(intList[i]) == int):
            if(intList[i] <= 0 or intlist[i] > maxInt):
                break
            else:
                print("Int out of bounds")
                return
        else:
            print("Incorrect input")
            return
    else:
        print("Not all numbers were postive")
        return
    return intList

def threadStart(function, arguments=None):
    cThread = threading.Thread(target=function)
    cThread.daemon = True
    cThread.start()

def sendWork(numbers):
    global clientID
    global worklist
    worklist.append(Work(numbers, time.time()))

    send({"user": "client", "id": clientID, "cmd": "work", "workLoad": numbers})


def send(jsonAbleString):
    global s
    s.send(json.dumps(jsonAbleString).encode())

def pause():
    time.sleep(2)
    print("break")

def startTCPListener(brokerIP=brokerIP, port=PORT):
    global s
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((brokerIP, PORT))
        join(s)
    except:
        traceback.print_exc()
        quit()
    partial = ""
    while True:
        try:
            data = s.recv(1024).decode().strip()
            #print("received", data)
            partial += data
            if partial[-1]=="}":
                data=partial
                partial = ""
            else:
                continue
            #print(data)
            #print(f"trying to open{data}")
            data = json.loads(data)
            try:
                if data["cmd"] == "result":
                    results.append(data["result"])
                    checkWorkList(data["result"])
                    #print(data)
                elif data["cmd"] == "workAccept":
                    pass
                    #print(data)
                elif data["cmd"] == "ping":
                    send("pong")
                    #print(data)
            except KeyError:
                continue
        except KeyboardInterrupt:
            s.close()
            break
        except Exception as e:
            print("TCPLISTENER CRASHED")
            traceback.print_exc()
            print(e)
            quit()
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
