import time
import math
import os
import socket
import json
import traceback
import threading

brokerIP="192.168.1.204"
PORT = 25566
ServerID = 1

def doTheWork(workID, workLoad):
    ## Takes in the workload, calculates the factors and sends the results to the broker
    try:
        print("starting work")
        results = factor(workLoad)
        print("work done returning it")
        sendResults(workLoad, results, workID)
        print("work sent back")
    except:
        traceback.print_exc()

def sendResults(number, results, workID):
    ## Sends the results to the broker
    global ServerID
    send({"user": "server", "id": ServerID, "cmd": "results", "workID": workID, "number": number, "results": results})

def acceptJob(number, workID):
    ## Sends an accept job message
    global ServerID
    send({"user": "server", "id": ServerID, "cmd": "accept", "workID": workID, "number": number})

def send(jsonAbleString):
    ## Send a tcp message containing a json package
    global s
    s.send(json.dumps(jsonAbleString).encode())

def startTCPListener(brokerIP=brokerIP, port=PORT):
    ## Listens for the TCP messages
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
            while True:
                data = s.recv(1024).decode()
                #print("received", data)
                partial += data
                if partial[-1]=="}":
                    data=partial
                    partial = ""
                else:
                    continue
                print(f"trying to open{data}")
                datas = [a+"}" for a in data.split("}")[:-1]]
                for data in datas:
                    data = json.loads(data)
                    try:
                        if data["cmd"]=="doWork":
                            workID = data["workID"]
                            workLoad = data["workload"]
                            acceptJob(workLoad, workID)
                            threadStart(doTheWork, (workID, workLoad))
                        elif data["cmd"]=="ping":
                            #print("Server sending a pong to the broker")
                            send({"cmd": "pong"})
                    except KeyError:
                        continue
                    except:
                        print("TCPLISTENER CRASHED")
                        traceback.print_exc()
        except KeyboardInterrupt:
            s.close()
            break
        except Exception as e:
            traceback.print_exc()
            print(e)
            quit()
            s.close()

def join(s):
    s.send(json.dumps({"user": "server", "cmd": "join", "id": serverID, "maxWork": 5*os.cpu_count()}).encode())

def readID(fileName="serverID"):
    ## Reads the server ID from a text file.
    try:
        f = open(fileName, "r")
        temp = f.read().strip()
        if(temp == ""):
            temp = None
        return temp
    except FileNotFoundError:
        print("new instance")
        return None

def writeID(clientID, fileName="serverID"):
    ## Writes the server ID into a text file
    f = open(fileName, "w")
    f.write(str(serverID))

def getServerID(brokerIP=brokerIP, port=PORT):
    ## Gets a new server ID from the broker
    global serverID
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((brokerIP, PORT))
        s.send(json.dumps({"user": "server", "cmd": "join", "id": readID(), "maxWork": os.cpu_count()}).encode())
        data = s.recv(1024).decode().strip()
        serverID = json.loads(data)["id"]
        writeID(serverID)
        s.close()


def factor(number):
    ## Calculates the factors of a number
    result = []
    displayed = []
    for i in range(1,math.ceil(number/2)+1):
        if 200*i//number not in displayed:
            displayed.append(200*i//number)
            print(f"working {number} {200*i//number}%")
        if number/i%1==0:
            result.append(i)
    result.append(number)
    return result

def threadStart(function, arguments=None):
    ## Starts a thread for a function
    if arguments:
        cThread = threading.Thread(target=function,args=arguments)
    else:
        cThread = threading.Thread(target=function)
    cThread.daemon = True
    cThread.start()

def main():
    getServerID()
    startTCPListener()


if __name__=="__main__":
    main()
