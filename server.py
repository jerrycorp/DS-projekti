import time
import math
import os

brokerIP="127.0.0.1"
PORT = 25566
ServerID = 1

def doTheWork(workID, workLoad):
    results = factor(workLoad)
    sendResults(workLoad, results, workID)

def sendResults(number, results, workID):
    global ServerID
    send({"user": "server", "id": ServerID, "cmd": "results", "workID": workID, "number": number, "results": results}

def send(jsonAbleString):
    global s
    s.send(json.dumps(jsonAbleString).encode())

def startTCPListener(brokerIP=brokerIP, port=PORT):
    global s
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((brokerIP, PORT))
                while True:## TODO: Create message handlers
                    data = s.recv(1024)
                    print("received", data.decode())
                    data = json.load(data)
                    if data["cmd"]=="doWork":
                        workID = data["workID"]
                        workLoad = data["workload"]
                        threadStart(doTheWork, (workID, workLoad))
                    if data["cmd"]=="ping":
                        send("pong")

        except KeyboardInterrupt:
            s.close()
            break
        except Exception as e:
            print(e)
            s.close()

def readID(fileName="serverID"):
    f = open(fileName, "r")
    temp = f.read().strip()
    if(temp == "")
        temp = getServerID()
    return temp

def writeID(clientID, fileName="serverID"):
    f = open(fileName, "w")
    f.write(string(serverID))

def getServerID(brokerIP=brokerIP, port=PORT):
    global serverID
    global s
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((brokerIP, PORT))
        s.send(json.dumps({"user": "server", "cmd": "join", "id": readID(), "maxWork": os.cpu_count()}).encode())
        data = s.recv(1024).decode().strip()
        serverID = json.loads(data)["id"]


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
