import time
import socket
import json
import threading
import time
from datetime import datetime
import traceback
import variables



#STATIC
brokerIP="127.0.0.1"
PORT = 25566
IDFilename = "ids.txt"
#VARIABLES
from variables import servers
from variables import clients
from variables import works
from variables import jobs
from variables import pendingJobs
#servers = []
#clients = []
#works = []
#jobs = []
#pendingJobs = {} # {time.time(), job}

## TODO: make client and server handle disconnects
## TODO: implement pendingjobs as a queue for jobs not accepted yet
## TODO: handle crashed servers
## TODO: clean up printing and input in client

def getNextID():#client/server
    ## Return next id based on the next avaiable id that is also written on disk
    try:
        file = open(IDFilename, "r")
    except FileNotFoundError:
        file = open(IDFilename, "w")
        file.write("0")
        file.close()
        file = open(IDFilename, "r")
    finally:
        id = int(file.read()) + 1
        file.close()
        file = open(IDFilename, "w")
        file.write(str(id))
        return id

class Work:
    def __init__ (self, work, clientID, workID=getNextID()):
        self.work = {}
        for w in work:
            self.work[w] = None
        self.clientID = clientID #123
        self.workID = workID
    def addResult(self, number, result):
        try:
            print("added result")
            self.work[number] = result
            if not None in list(self.work.values()):
                global clients
                for client in clients:
                    if client.clientID==self.clientID:
                        client.sendResults(self)
                        client.works.remove(self)
        except:
            traceback.print_exc()
        ## TODO: check if done

class job:
    def __init__ (self, number, clientID, workID):
        self.number =  number
        self.clientID = clientID
        self.workID = workID

class Server:
    def __init__ (self, serverID, sock, maxWork):
        self.type="server"
        self.serverID = serverID
        self.sock = sock
        self.jobs = []
        self.maxWork = maxWork
        self.lastActive = time.time()
        self.lastPinged = time.time()
        self.send({})
        global servers
        servers.append(self)
    def sendJob(self, job):
        self.jobs.append(job)
        self.send({"cmd": "doWork", "workID": job.workID, "workload": job.number })
    def send(self, jsonAbleString):
        jsonAbleString["id"] = self.serverID
        jsonAbleString["user"] = "server"
        self.sock.send(json.dumps(jsonAbleString).encode())
    def sendPing(self):
        print(f"pinging {self.serverID}")
        self.lastPinged = time.time()
        self.send({"cmd": "ping"})
    def removeJob(self, workID, number, results):
        for job in jobs:
            if job.workID==workID and job.number==number:
                self.jobs.remove(job)

class Client:
    def __init__ (self, clientID, sock):
        self.type="client"
        self.clientID = clientID
        self.sock = sock
        self.works = []
        self.lastActive = time.time()
        self.lastPinged = time.time()
        self.send({})
        global clients
        clients.append(self)
    def addWork(self, work):
        global jobs
        self.works.append(work)
        for number in work.work:
            jobs.append(job(number, self.clientID, work.workID))
    def send(self, jsonAbleString):
        jsonAbleString["id"] = self.clientID
        jsonAbleString["user"] = "client"
        self.sock.send(json.dumps(jsonAbleString).encode())
    def sendResults(self, work):
        self.send({"cmd": "result", "result": work.work})
    def sendPing(self):
        print(f"pinging {self.clientID}")
        self.lastPinged = int(time.time())
        self.send({"cmd": "ping"})


def handlerLoop(user, userType):
    c = user.sock
    try:
        print(f"Looping {userType} message:")
        while True:
            data = c.recv(1024).decode().strip()
            if not data:
                c.close()
            write(c.getpeername()[0] + ",  "  + data)
            try:
                data = json.loads(data)
                if userType=="client":
                    clientCommandParser(data, user)
                elif userType=="server":
                    serverCommandParser(data, user)
            except Exception as e:
                traceback.print_exc()
                print(e)
    except UnicodeDecodeError:
        write("UnicodeDecodeError")
        c.close()
    except OSError:
        write("OSError")
        if user.type == "server":
            servers.remove(user)
        elif user.type == "client":
            clients.remove(user)
        c.close()

def clientCommandParser(data, client):
    print("parsing client message")
    if data["cmd"]=="ping":
        client.send({"cmd": "pong"})
    elif data["cmd"]=="pong":
        client.lastActive = int(time.time())
    elif data["cmd"]=="work":
        print("adding work")
        client.lastActive = int(time.time())
        client.addWork(Work(data["workLoad"], client.clientID))
    ## TODO: parse commands

def serverCommandParser(data, server):
    print("parsing server message")
    global clients
    if data["cmd"]=="ping":
        client.send({"cmd": "pong"})
    if data["cmd"]=="pong":
        server.lastActive = int(time.time())
    if data["cmd"] == "results":
        print("received results")
        server.lastActive = int(time.time())
        workID = data["workID"]
        server.removeJob(data["workID"], data["number"], data["results"])
        for cli in clients:
            for wor in cli.works:
                if(wor.workID == workID):
                    wor.addResult(data["number"], data["results"])
                    #j.sendResults(data["results"])

def initialHandler(c, a):
    try:
        print("REVEIVED CONNECTION")
        message = c.recv(1024).decode().strip()
        if not message:
            c.close()
        write(c.getpeername()[0] + ",  "  + message)
        try:
            data = json.loads(message)
            if data["cmd"] == "join":
                if data["user"] == "client":
                    if data["id"]:
                        print("OLD CONNECTION")
                        handlerLoop(Client(data["id"], c), "client")
                    else:
                        handlerLoop(Client(getNextID(), c), "client")
                elif data["user"] == "server":
                    maxWork = data["maxWork"]
                    if data["id"]:
                        print("OLD CONNECTION")
                        handlerLoop(Server(data["id"], c, maxWork), "server")
                    else:
                        handlerLoop(Server(getNextID(), c, maxWork), "server")
        except Exception as e:
            traceback.print_exc()
            print("crashed at tcp accept:", e)
    except UnicodeDecodeError:
        write("UnicodeDecodeError")
        c.close()
    except OSError:
        write("OSError")
        c.close()

def send(jsonAbleString, s):
    s.send(json.dumps(jsonAbleString).encode())

def TCPListener(port=PORT):
    try:
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.bind(('0.0.0.0',port))
        sock.listen(1)
        print("READY")
        while True:
            c, a = sock.accept()
            threadStart(initialHandler, (c,a))
    except KeyboardInterrupt:
        if type(c) != type(1):
            c.close()
            write("closed connection")
        write("Console closed by keyboard")
    except OSError:
        time.sleep(1)
        print("port busy")
        main()
    except:
        traceback.print_exc()
        if type(c) != type(1):
            c.close()
        write("Unexpected error:" + str(sys.exc_info()[0]))
        run = False

def threadStart(function, arguments=None):
    if arguments:
        cThread = threading.Thread(target=function,args=arguments)
    else:
        cThread = threading.Thread(target=function)
    cThread.daemon = True
    cThread.start()

def write(stuff):
    with open(datetime.now().strftime("%Y-%m-%d.log"),"a") as log:
        print(datetime.now().strftime("%d/%m/%Y, %H:%M:%S,"), stuff, "\n", end='')
        log.write(datetime.now().strftime("%d/%m/%Y, %H:%M:%S,"))
        log.write(str(stuff) + "\n")

def workHandler():
    global servers
    global jobs
    global s
    global clients
    while True:
        time.sleep(10)
        print("checking for work")
        print("jobs", jobs)
        print("clients:", clients)
        print("servers:", servers)
        for n in servers:
            ## TODO: go through pendingJobs. if taken more than 10 seconds move back to jobs
            if((n.lastActive - n.lastPinged) > 10):
                n.sendPing()

            if(len(jobs) > 0):
                print("adding work")
                if(n.maxWork > len(n.jobs)):### TODO: make it give max amount of jobs.
                    print("found server with free space")
                    job = jobs.pop(0)
                    try:
                        n.sendJob(job)
                    except:
                        traceback.print_exc()



def main():
    threadStart(TCPListener)
    threadStart(workHandler)
    try:
        while True:
            input()
    except KeyboardInterrupt:
        print("bye bye")
        quit()
        ## TODO: close things ?

if __name__ == "__main__":
    main()
