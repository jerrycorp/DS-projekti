import time
import socket
import json
import threading
import time

#STATIC
brokerIP="127.0.0.1"
PORT = 25566
IDFilename = "ids.txt"
#VARIABLES
servers = []
clients = []
works = []
jobs = []
pendingJobs = {} # {time.time(), job}


class Work:
    def __init__ (self, work, clientID, workID):
        self.work =  work #{1: None,2: None ,3: None, 4: None}
        self.clientID = clientID #123
        self.workID = workID
    def addResult(self, number, result):
        self.work[number] = result
        if not None in list(self.work.values):
            global clients
            for client in clients:
                if client.clientID==self.clientID:
                    client.sendResults()
                    client.works.remove(self)
        ## TODO: check if done

class job:
    def __init__ (self, number, clientID, workID):
        self.number =  number
        self.clientID = clientID
        self.workID = workID

class Server:
    def __init__ (self, serverID, serverSocket, maxWork):
        self.serverID = serverID
        self.serverSocket = serverSocket
        self.jobs = []
        self.maxWork = maxWork
        self.lastActive = time.time()
        self.lastPinged = None
    def sendJob(self, job):
        self.jobs.append(job)
        self.send({"cmd": "doWork", "workID": job.workID, "workload": job.number })
    def send(self, jsonAbleString):
        jsonAbleString[id] = self.serverID
        jsonAbleString[user] = "server"
        self.serverSocket.send(json.dump(jsonAbleString).encode())
    def sendPing(self):
        self.lastPinged = time.time()
        self.send({"cmd": "ping"})
    def removeJob(self, workID, number, results):
        for job in jobs:
            if job.workID==workID and job.number==number:
                self.jobs.remove(job)

class Client:
    def __init__ (self, clientID, clientSocket):
        self.clientID = clientID
        self.clientSocket = clientSocket
        self.works = []
        self.lastActive = time.time()
        self.lastPinged = None
    def addWork(self, work):
        global jobs
        self.works.append(work)
        for number in work.work:
            jobs.append(job(number, self.clientID, self.workID))
    def send(self, jsonAbleString):
        jsonAbleString[id] = self.clientID
        jsonAbleString[user] = "client"
        self.clientSocket.send(json.dump(jsonAbleString).encode())
    def sendResults(self, work):
        self.send({"cmd": "result", "result": work.work})
    def sendPing(self):
        self.lastPinged = int(time.time())
        self.send({"cmd": "ping"})

def getNextID():#client/server
    ## Return next id based on the next avaiable id that is also written on disk
    try:
        file = open(IDFilename, "r")
    except FileNotFoundError:
        file = open(IDFilename, "w")
        file.write(0)
        file.close()
        file = open(IDFilename, "r")
    finally:
        id = file.read() + 1
        file.close()
        file = open(IDFilename, "w")
        file.write(id)
        return id

def handlerLoop(user, userType):
    try:
        print(f"Looping {userType} messages")
        while True:
            data = c.recv(1024).decode().strip()
            if not data:
                c.close()
            write(c.getpeername()[0] + ",  "  + data)
            try:
                data = json.loads(data)
                if userType=="client":
                    clientCommandParser(data, client)
                elif userType=="server":
                    serverCommandParser(data, client)
            except:
                pass
    except UnicodeDecodeError:
        write("UnicodeDecodeError")
        c.close()
    except OSError:
        write("OSError")
        c.close()

def clientCommandParser(data, client):
    if data["cmd"]=="ping":
        client.send({"cmd": "pong"})
    elif data["cmd"]=="pong":
        client.lastActive = int(time.time())
    elif data["cmd"]=="work":
        client.lastActive = int(time.time())
        client.addWork(data["workLoad"])
    ## TODO: parse commands

def serverHandlerLoop(data, server):
    global clients
    if data["cmd"]=="ping":
        client.send({"cmd": "pong"})
    if data["cmd"]=="pong":
        server.lastActive = int(time.time())
    if data["cmd"] == "results":
        server.lastActive = int(time.time())
        workID = data["id"]
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
                        handlerLoop(client(data["id"], c))
                    else:
                        handlerLoop(client(getNextID(), c))
                elif data["user"] == "server":
                    maxWork = data["maxWork"]
                    if data["id"]:
                        print("OLD CONNECTION")
                        handlerLoop(client(data["id"], c, maxWork))
                    else:
                        handlerLoop(client(getNextID(), c, maxWork))
        except Exception as e:
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
        time.sleep(1)
        for n in servers:
            ## TODO: go through pendingJobs. if taken more than 10 seconds move back to jobs
            if((n.lastActive - n.lastPinged) > 10):
                n.sendPing()

            if(len(jobs > 0)):
                if(n.maxwork > len(n.jobs)):
                    job = jobs.pop(0)
                    try:
                        server.sendJob(job)
                    except:
                        pass



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
