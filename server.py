import time
import math
import os
import socket
import json
import traceback
import threading

brokerIP="127.0.0.1"
PORT = 25566
ServerID = 1

def doTheWork(workID, workLoad):
    try:
        print("starting work")
        results = factor(workLoad)
        print("work done returning it")
        sendResults(workLoad, results, workID)
        print("work sent back")
    except:
        traceback.print_exc()

def sendResults(number, results, workID):
    global ServerID
    send({"user": "server", "id": ServerID, "cmd": "results", "workID": workID, "number": number, "results": results})

def send(jsonAbleString):
    global s
    s.send(json.dumps(jsonAbleString).encode())

def startTCPListener(brokerIP=brokerIP, port=PORT):
    global s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((brokerIP, PORT))
    join(s)
    while True:
        try:
            while True:## TODO: Create message handlers
                data = s.recv(1024)
                print("received", data.decode())
                data = json.loads(data)
                try:
                    if data["cmd"]=="doWork":
                        workID = data["workID"]
                        workLoad = data["workload"]
                        threadStart(doTheWork, (workID, workLoad))
                    elif data["cmd"]=="ping":
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
            s.close()

def join(s):
    s.send(json.dumps({"user": "server", "cmd": "join", "id": serverID, "maxWork": os.cpu_count()}).encode())

def readID(fileName="serverID"):
    f = open(fileName, "r")
    temp = f.read().strip()
    if(temp == ""):
        temp = None
    return temp

def writeID(clientID, fileName="serverID"):
    f = open(fileName, "w")
    f.write(str(serverID))

def getServerID(brokerIP=brokerIP, port=PORT):
    global serverID
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((brokerIP, PORT))
        s.send(json.dumps({"user": "server", "cmd": "join", "id": readID(), "maxWork": os.cpu_count()}).encode())
        data = s.recv(1024).decode().strip()
        serverID = json.loads(data)["id"]
        writeID(serverID)


def factor(number):
    result = []
    for i in range(1,math.ceil(number/2)+1):
        if number/i%1==0:
            result.append(i)
    result.append(number)
    return result

def threadStart(function, arguments=None):
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
