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
brokerIP="192.168.1.204"
PORT = 25566
clientID = None
results = []
worklist = []
maxInt = 15000000

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
        rand = random.randint(minInt, maxInt)
        while rand in input:
            rand = random.randint(minInt, maxInt)
        input.append(rand)
    return input

def UILoop():
    #Simple console ui
    while True:
        userInput = input("Select if you want to view results: v, add numbers to be factored: a or exit: e").lower().split()
        decision = uiDecision(userInput)
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
            sendWork(randomInputGenerator(1000000,100))
        elif decision =="file":
            if len(userInput) > 1:
                requestFromFile(userInput[1])
            else:
                print("Use: f {filename}")

def requestFromFile(filename):
    ## Reads integers from a file to be sent to the broker
    try:
        with open(filename) as file:
            numbers = list(map(int, file.readlines()))
    except TypeError:
        print("Input file contains non-integer values!")
        return
    except FileNotFoundError:
        print("File not found!")
        return
    sendWork(numbers)

def printResults():
    ## Prints the results
    if(len(results) > 0):
        for n in results:
            print(n)
    else:
        print("No results yet. Try again later.")

def uiDecision(userInput):

    print("Hello world!")
    if(userInput[0][0] == "a"):
        return "work"
    if(userInput[0][0] == "v"):
        return "results"
    if(userInput[0][0] == "e"):
        return "EXIT"
    if(userInput[0][0] == "r"):
        return "random"
    if(userInput[0][0] == "s"):
        return "small"
    if(userInput[0][0] == "f"):
        return "file"

def requestNumbers():
    ## Requests input from user and checks if the input is valid
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
        if(type(intList[i]) == int):
            if(len(intList) == len(set(intList))):
                if(intList[i] <= 0 or intList[i] > maxInt):
                    print("Int out of bounds")
                    return
                #else:
                    #print("Int out of bounds")
                    #return
            else:
                print("Input shall not include duplicate INTs")
                return
        else:
            print("Incorrect input")
            return
    #else:
    #    print("Not all numbers were postive")
    #    return
    return intList

def threadStart(function, arguments=None):
    ## Starts a thread for a function
    cThread = threading.Thread(target=function)
    cThread.daemon = True
    cThread.start()

def sendWork(numbers):
    ## Send work to the broker
    global clientID
    global worklist
    worklist.append(Work(numbers, time.time()))

    send({"user": "client", "id": clientID, "cmd": "work", "workLoad": numbers})


def send(jsonAbleString):
    ## Send a tcp message containing a json package to the broker.
    global s
    s.send(json.dumps(jsonAbleString).encode())

def pause():
    time.sleep(2)
    print("break")

def startTCPListener(brokerIP=brokerIP, port=PORT):
    ## Listens for tcp messages
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
                if data["cmd"] == "result": # Received results
                    results.append(data["result"]) # Add the results to the result list
                    checkWorkList(data["result"])
                    #print(data)
                elif data["cmd"] == "workAccept": # Received work accept message
                    pass
                    #print(data)
                elif data["cmd"] == "ping": # Received ping
                    send("pong") # Respond to ping
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
            time.sleep(2)

def readID(fileName="clientID"):
    ## Reads the client ID from a text file
    with open(fileName, "r") as f:
        temp = f.read().strip()
        if(temp == ""):
            temp = None
        return temp

def writeID(clientID, fileName="clientID"):
    ## Writes the client ID into a text file
    f = open(fileName, "w")
    f.write(str(clientID))

def getClientID(brokerIP=brokerIP, port=PORT):
    ## Gets a new client ID from the broker
    global clientID
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((brokerIP, PORT))
        s.send(json.dumps({"user": "client", "cmd": "join", "id": readID()}).encode())
        data = s.recv(1024).decode().strip()
        clientID = json.loads(data)["id"]
        writeID(clientID)
        s.close()

def join(s):
    ## Send initial message to the broker
    s.send(json.dumps({"user": "client", "cmd": "join", "id": clientID}).encode())

def main():
    getClientID()
    threadStart(startTCPListener)
    time.sleep(1)
    UILoop()

if __name__ == "__main__":
    main()
