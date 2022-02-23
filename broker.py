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
ready = False


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
        #print(f"created work with {len(self.work.keys())} items")
    ## Add a result to a work object and if all the results are added, send a result to the client
    def addResult(self, number, result):
        try:
            #print("added result")
            self.work[number] = result
            if not None in list(self.work.values()):
                global clients
                for client in clients:
                    if client.clientID==self.clientID:
                        client.sendResults(self)
                        client.works.remove(self)
        except:
            traceback.print_exc()

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
        #self.pendingJobs = []
        self.maxWork = maxWork
        self.lastActive = time.time()
        self.lastPinged = time.time()
        self.send({})#sends id
        global servers
        servers.append(self)
    #Send a job to the server
    def sendJob(self, job):
        global pendingJobs
        self.jobs.append(job)
        pendingJobs.append(PendingJob(job, time.time()))
        self.send({"cmd": "doWork", "workID": job.workID, "workload": job.number })
    #Send a tcp message to a server containing a json package
    def send(self, jsonAbleString):
        jsonAbleString["id"] = self.serverID
        jsonAbleString["user"] = "server"
        self.sock.send(json.dumps(jsonAbleString).encode())
    #Send a ping to a server
    def sendPing(self):
        #print(f"pinging {self.serverID}")
        self.lastPinged = time.time()
        self.send({"cmd": "ping"})
    #Remove a job from a server
    def removeJob(self, workID, number, results):
        for job in self.jobs:
            if job.workID==workID and job.number==number:
                self.jobs.remove(job)
                #pendingJobs.remove(job)
    def delete(self):
        global servers
        global jobs
        jobs += self.jobs
        servers.remove(self)
        try:
            self.sock.close()
        except:
            traceback.print_exc()

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
    #Add work for a client
    def addWork(self, work):
        global jobs
        self.works.append(work)
        for number in work.work:
            jobs.append(Job(number, self.clientID, work.workID))
    #Send a tcp message to a client containing a json package
    def send(self, jsonAbleString):
        jsonAbleString["id"] = self.clientID
        jsonAbleString["user"] = "client"
        self.sock.send(json.dumps(jsonAbleString).encode())
    #Send results to a client
    def sendResults(self, work):
        #print(f"sending result with {len(work.work.keys())}")
        self.send({"cmd": "result", "result": work.work})
    #Send a ping to a client
    def sendPing(self):
        #print(f"pinging {self.clientID}")
        self.lastPinged = int(time.time())
        self.send({"cmd": "ping"})

#Main loop for handling the messages from clients and servers
def handlerLoop(user, userType):
    c = user.sock
    partial = ""
    try:
        #print(f"Looping {userType} message:")
        while True:
            data = c.recv(1024).decode().strip()
            if not data:
                c.close()
            #write(c.getpeername()[0] + ",  "  + data)
            #print("received", data)
            partial += data
            if partial[-1]=="}":
                data=partial
                partial = ""
            else:
                continue
            datas = [a+"}" for a in data.split("}")[:-1]]
            for data in datas:
                #print(f"trying to open{data}")
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
    except IndexError:
        if user.type == "server":
            user.delete()
        elif user.type == "client":
            clients.remove(user)
    except OSError:
        write("OSError")
        traceback.print_exc()
        try:
            c.close()
        except:
            print("failed to close socket after handler crash")
        if user.type == "server":
            user.delete()
        elif user.type == "client":
            clients.remove(user)

    except:
        traceback.print_exc()
        try:
            c.close()
        except:
            print("failed to close socket after handler crash")
        if user.type == "server":
            user.delete()
        elif user.type == "client":
            clients.remove(user)


#Handles the messages that come from the clients
def clientCommandParser(data, client):
    #print("parsing client message")
    if data["cmd"]=="ping": #Received ping
        client.send({"cmd": "pong"}) #Respond to ping
    elif data["cmd"]=="pong": #Received pong
        client.lastActive = int(time.time()) #Refresh the lastActive timestamp
    elif data["cmd"]=="work": #Received work from the client
        #print("adding work")
        client.lastActive = int(time.time()) #Refresh the lastActive timestamp
        client.addWork(Work(data["workLoad"], client.clientID)) #Add the work to client's work list

#Handles the messages that come from the servers
def serverCommandParser(data, server):
    #print("parsing server message")
    global clients
    if data["cmd"]=="ping": #Received ping
        client.send({"cmd": "pong"}) #Respond to ping
    if data["cmd"]=="pong": #Received pong
        server.lastActive = int(time.time()) #Refresh the lastActive timestamp
    if data["cmd"] == "results": #Received results
        #print("received results")
        server.lastActive = int(time.time()) #Refresh the lastActive timestamp
        workID = data["workID"]
        server.removeJob(data["workID"], data["number"], data["results"]) # Remove the completed job from job list
        #Go through the clients and add the result to the client that gave the job
        for cli in clients:
            for wor in cli.works:
                if(wor.workID == workID):
                    wor.addResult(data["number"], data["results"])
                    #j.sendResults(data["results"])
    if data["cmd"] == "accept": #Received the job accept message
        global pendingJobs
        #removable = []
        #Remove the accepted job from the pendingJobs list
        for pendingjob in pendingJobs:
            job = pendingjob.job
            if job.workID==data["workID"] and job.number==data["number"]:
                #print("Pendingjob " + str(data["number"]) + " accepted by server " + str(server.serverID))
                break
                #removable.append(pendingjob)
        else:
            print("Couldn't find accepted pending job in pendingJobs")
            return
        pendingJobs.remove(pendingjob)
        #pendingJobs = [x for x in pendingJobs if x not in removable]


def initialHandler(c, a):
    try:
        #print("REVEIVED CONNECTION")
        message = c.recv(1024).decode().strip()
        if not message:
            c.close()
        #write(c.getpeername()[0] + ",  "  + message)
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

#Listens for tcp messages
def TCPListener(port=PORT):
    global sock
    try:
        #Create a socket and start listening
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.bind(('0.0.0.0',port))
        sock.listen(1)
        print("READY")
        global ready
        ready = True
        while True:
            #If a message is recieved, forward it to the initial message handler
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
        time.sleep(1)
        TCPListener()
        #main()
    except:
        traceback.print_exc()
        if type(c) != type(1):
            c.close()
        write("Unexpected error:" + str(sys.exc_info()[0]))
        run = False

#Starts a thread for a function
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
    while True:
        #time.sleep(0.5)
        """
        #Go through the clients
        #for m in clients:
            #if((time.time() - m.lastActive) > 5 and (time.time() - m.lastPinged) > 1):
                #print("Broker sending ping to a client")
                #m.sendPing()
        """
        #Go through the servers
        for n in servers:
            #print(f"server has {len(n.jobs)} currently underwork")
            # Go through pendingJobs. if taken more than 10 seconds move back to jobs
            t = time.time()
            #removable = []
            removed_pjob = None
            for pjob in pendingJobs:
                #print(pjob)
                #print(pjob.time - t)
                if pjob.time - t < -10:
                    #print("Pending job timeout, returning job to job list!")
                    jobs.append(pjob.job)
                    #removable.append(pjob)
                    removed_pjob = pjob
                    break
            if removed_pjob != None:
                pendingJobs.remove(removed_pjob)

            #pendingJobs = [x for x in pendingJobs if x not in removable]

            #Remove servers which were not active within the last 10 seconds
            if((time.time() - n.lastActive) > 10):
                n.delete()
                #print("Removed an inactive server")
                #print(f"last active {time.time() - n.lastActive}")

            #Ping servers which were not pinged within the last 5 seconds
            if((time.time() - n.lastActive) > 5 and (time.time() - n.lastPinged) > 1):
                #print("Broker sending ping to a server")
                n.sendPing()

            #Check if jobs are available and give them to the available servers
            if(len(jobs) > 0):
                maxWork = n.maxWork
                currentWork = len(n.jobs)
                #print("adding work")
                #Fill the server with work if it isn't already full
                if(maxWork > currentWork):
                    #print("found server with free space")
                    for i in range(maxWork-currentWork):
                        if len(jobs) == 0:
                            break
                        #print(f"adding work to server with {len(n.jobs)}")
                        job = jobs.pop(0)#check if empty / only add as many work as there are
                        try:
                            n.sendJob(job)
                        except:
                            traceback.print_exc()

#Reads the amount of tasks, servers and clients and writes them in a log file every second.
def evaluation():
    while True:
        time.sleep(1)
        #print("Writing in evaluation file...")

        numberOfServers = len(servers)
        numberOfClients = len(clients)
        numberOfpendingJobs = len(pendingJobs)
        numberOfWaitingJobs = len(jobs)
        numberOfJobsOnserver = []
        for n in servers:
            numberOfJobsOnserver.append(len(n.jobs))
        jsonAbleString = {"timestamp": str(datetime.now()), "servers": numberOfServers, "clients": numberOfClients, "jobs": numberOfJobsOnserver, "pendingJobs": numberOfpendingJobs, "waitingJobs": numberOfWaitingJobs}
        with open("evaluation.txt", "a") as f:
            f.write("\n")
            f.write(json.dumps(jsonAbleString))

def printStatus():
    oldPrint = ""
    while True:
        time.sleep(1)
        newPrint =f"""
checking for work
pending jobs: {len(pendingJobs)}
jobs: {len(jobs)}
clients: {len(clients)}
servers: {len(servers)}
"""

        if newPrint != oldPrint:
            print(newPrint)
            oldPrint = newPrint
        else:
            pass

def main():
    threadStart(TCPListener)
    while not ready:
        time.sleep(0.1)
    threadStart(workHandler)
    threadStart(evaluation)
    threadStart(printStatus)
    try:
        while True:
            input()
    except KeyboardInterrupt:
        print("bye bye")
        for server in servers:
            try:
                server.sock.close()
            except:
                pass
        for client in clients:
            try:
                client.sock.close()
            except:
                pass
        try:
            sock.close()
        except:
            pass
        time.sleep(2)
        quit()

if __name__ == "__main__":
    main()
