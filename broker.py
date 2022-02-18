import time
import socket
import json
import threading
import time
from datetime import datetime
import traceback
from dataclasses import dataclass



#STATIC
brokerIP="127.0.0.1"
PORT = 25566
IDFilename = "ids.txt"
#VARIABLES
servers = []
clients = []
works = []
jobs = [] #unshared jobs
pendingJobs = [] # {"job": job, "time":time.time()}


## TODO: some jobs are lost on the way to the client
## TODO: make client and server handle disconnects
## TODO: handle crashed servers

def getNextID():#client/server/work
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
        print(f"created work with {len(self.work.keys())} items")
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

class Job:
    def __init__ (self, number, clientID, workID):
        self.number =  number
        self.clientID = clientID
        self.workID = workID

@dataclass
class PendingJob:
    job: Job
    time: int

class Server:
    def __init__ (self, serverID, sock, maxWork):
        self.type="server"
        self.serverID = serverID
        self.sock = sock
        self.jobs = []
        self.pendingjobs = []
        self.maxWork = maxWork
        self.lastActive = time.time()
        self.lastPinged = time.time()
        self.send({})
        global servers
        servers.append(self)
    def sendJob(self, job):
        global pendingjobs
        self.jobs.append(job)
        pendingJobs.append(PendingJob(job, time.time()))
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
        for job in self.jobs:
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
            jobs.append(Job(number, self.clientID, work.workID))
    def send(self, jsonAbleString):
        jsonAbleString["id"] = self.clientID
        jsonAbleString["user"] = "client"
        self.sock.send(json.dumps(jsonAbleString).encode())
    def sendResults(self, work):
        print(f"sending result with {len(work.work.keys())}")
        self.send({"cmd": "result", "result": work.work})
    def sendPing(self):
        print(f"pinging {self.clientID}")
        self.lastPinged = int(time.time())
        self.send({"cmd": "ping"})


def handlerLoop(user, userType):
    c = user.sock
    partial = ""
    try:
        print(f"Looping {userType} message:")
        while True:
            data = c.recv(1024).decode().strip()
            if not data:
                c.close()
            write(c.getpeername()[0] + ",  "  + data)
            print("received", data)
            partial += data
            if partial[-1]=="}":
                data=partial
                partial = ""
            else:
                continue
            datas = [a+"}" for a in data.split("}")[:-1]]
            for data in datas:
                print(f"trying to open{data}")
                data = json.loads(data)
                try:
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
    if data["cmd"] == "accept":
        global pendingJobs
        removable = []
        for pendingjob in pendingJobs:
            job = pendingjob.job
            if job.workID==data["workID"] and job.number==data["number"]:
                removable.append(pendingjob)
        pendingJobs = [x for x in pendingJobs if x not in removable]


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
    global pendingJobs
    oldPrint = ""
    while True:
        time.sleep(0.5)
        if str([pendingJobs, jobs, servers, clients]) != oldPrint:
            print("checking for work")
            print("pending jobs:", pendingJobs)
            print("jobs:", jobs)
            print("clients:", clients)
            print("servers:", servers)
            oldPrint = str([pendingJobs, jobs, servers, clients])
        else:
            pass

        for n in servers:
            print(f"server has {len(n.jobs)} currently underwork")
            # Go through pendingJobs. if taken more than 10 seconds move back to jobs
            t = time.time()

            removable = []
            for pjob in pendingJobs:
                #print(pjob)
                if pjob.time - t > 10:
                    jobs.append(pjob.job)
                    removable.append(pjob)

            pendingJobs = [x for x in pendingJobs if x not in removable]

            if((n.lastActive - n.lastPinged) > 10):
                n.sendPing()

            #Check if jobs are available
            if(len(jobs) > 0):
                maxWork = n.maxWork
                currentWork = len(n.jobs)
                print("adding work")
                #Fill the server with work if it isn't already full
                if(maxWork > currentWork):
                    print("found server with free space")
                    for i in range(maxWork-currentWork):
                        if len(jobs) == 0:
                            break
                        job = jobs.pop(0)#check if empty / only add as many work as there are
                        try:
                            n.sendJob(job)
                        except:
                            traceback.print_exc()

def evaluation():
    while True:
        time.sleep(1)
        print("Writing in evaluation file...")

        numberOfServers = len(servers)
        numberOfClients = len(clients)
        numberOfPendingJobs = len(pendingJobs)
        numberOfWaitingJobs = len(jobs)
        numberOfJobsOnserver = []
        for n in servers:
            numberOfJobsOnserver.append(len(n.jobs))
        jsonAbleString = {"timestamp": str(datetime.now()), "servers": numberOfServers, "clients": numberOfClients, "jobs": numberOfJobsOnserver, "pendingJobs": numberOfPendingJobs, "waitingJobs": numberOfWaitingJobs}
        with open("evaluation.txt", "a") as f:
            f.write("\n")
            f.write(json.dumps(jsonAbleString))

def main():
    threadStart(TCPListener)
    threadStart(workHandler)
    threadStart(evaluation)
    try:
        while True:
            input()
    except KeyboardInterrupt:
        print("bye bye")
        quit()
        ## TODO: close things ?

if __name__ == "__main__":
    main()
