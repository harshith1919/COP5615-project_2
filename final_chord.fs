// Learn more about F# at http://docs.microsoft.com/dotnet/fsharp


open System
open Akka.FSharp
open Akka.Actor
open System.Threading

type MasterMessageTypes = 
    | SetParameters of String
    | ConvergenceCounter of int

type WorkerMessageTypes =
    | Print
    | InitialiseID of int * int * IActorRef
    | Create
    | Join of IActorRef * int
    | SetSucc of IActorRef * int
    | Stabilize of int * IActorRef * int //0 if stabilise is called with no info about succ.pred, 1 if called with info about succ.pred
    | SendPred of IActorRef
    | Notify of IActorRef * int
    | SetPred of IActorRef * int
    | FindSuccessor of IActorRef * int
    | FindKeySuccessor of IActorRef * int * int
    | CountHops of int
    | FindSuccessorFingers of int * int * IActorRef
    | FixFingers
    | SaveFingers of int * int * IActorRef
    | StabilizeScheduler
    | NodeScheduler
    | KeyFindRequest


let system = ActorSystem.Create("Chord")


let Worker(mailbox: Actor<_>)=

    let m = 20
    let mutable id = -1
    let mutable succId = -1
    let mutable succRef = null
    let mutable predId = -1
    let mutable predRef = null
    let mutable requests = 0
    let mutable sumHops = 0
    let mutable fingerTable = Array.create m 0
    let mutable refTable = Array.create m null
    let mutable masterRef = null
    let mutable next = 0


    let rec loop()= actor{
       let! msg = mailbox.Receive()
       let sender = mailbox.Sender()
       let self = mailbox.Self

       match msg with
        | Print ->
            printfn "%i - %i %i %i Hops = %i\n" fingerTable.[0] predId id succId sumHops
            for i in [0..(m-1)] do
                printfn "%i - %i" i fingerTable.[i]

        | InitialiseID (i,n,mref) ->
            id <- i
            requests <- n
            masterRef <- mref

        | Create ->
            succId <- id
            succRef <- mailbox.Self
            fingerTable.[0] <- id
            refTable.[0] <- self 
            predId <- -1
            predRef <- null

        | Join (jRef, jId) ->
            if id <> succId then
                self <! FindSuccessor(jRef, jId)
            else
                succId <- jId
                succRef <- jRef
                fingerTable.[0] <- jId
                refTable.[0] <- jRef
                jRef <! SetSucc(self,id)
                jRef <! SetPred(null,-1)

        | SetSucc (sRef,sId) ->
            succRef <- sRef
            succId <- sId
            for i in [0..(m-1)] do
                fingerTable.[i] <- sId
                refTable.[i] <- sRef  

        | SetPred (pRef,pId) ->
            predRef <- pRef
            predId <- pId
                
        | FindSuccessor (jRef,jId) ->
            if(id>succId && jId>id && jId>=succId) then
                jRef <! SetSucc(succRef,succId)
            elif(id>succId && jId<id && jId<=succId) then
                jRef <! SetSucc(succRef,succId)
            elif(jId>id && jId<=succId) then
                jRef <! SetSucc(succRef,succId)
            else
                succRef <! FindSuccessor(jRef,jId)

        | FindKeySuccessor (jRef,jId,hopCount) ->
            if id = jId then
                jRef <! CountHops hopCount
            elif(id>succId && jId>id && jId>=succId) then
                jRef <! CountHops hopCount
            elif(id>succId && jId<id && jId<=succId) then
                jRef <! CountHops hopCount
            elif(jId>id && jId<=succId) then
                jRef <! CountHops hopCount

            else
                let mutable i = m-1
                while i>=0 do
                    let mutable finger = fingerTable.[i]
                    let mutable fingerRef = refTable.[i]
                    if (id>jId && finger>id && finger>jId) then
                        fingerRef <! FindKeySuccessor(jRef,jId, hopCount+1)
                        
                        i <- -1
                    elif (id>jId && finger<id && finger<jId) then
                        fingerRef <! FindKeySuccessor(jRef,jId, hopCount+1)
                        i <- -1
                    elif (finger>id && finger<jId) then
                        fingerRef <! FindKeySuccessor(jRef,jId, hopCount+1)
                        i <- -1
                    i<-i-1

        | CountHops hopCount->
            if requests>0 then
                sumHops <- sumHops+hopCount
                requests <- requests-1
                
        | FindSuccessorFingers (i,jId,jRef) ->
            if(id>succId && jId>id && jId>=succId) then
                jRef <! SaveFingers(i,succId,succRef)
            elif(id>succId && jId<id && jId<=succId) then
                jRef <! SaveFingers(i,succId,succRef)

            elif(jId>id && jId<=succId) then
                jRef <! SaveFingers(i,succId,succRef)
            else
                succRef <! FindSuccessorFingers(i,jId,jRef)

        | FixFingers ->  
            
            next <- next+1
            if (next > (m-1)) then
                next <- 0
            let searchNodeId = (id + pown 2 next) % (pown 2 m)
            self <! FindSuccessorFingers(next,searchNodeId,self)
            

        | SaveFingers (i,fingerId,fingerRef) ->
            fingerTable.[i] <- fingerId
            refTable.[i] <- fingerRef

        | Stabilize (nodeId, nodeRef, case) ->      
            if case = 0 then
                succRef <! SendPred(self)
            else
                if nodeId <> -1 then
                    if (id>succId && nodeId>id && nodeId>succId) then
                        succId <- nodeId
                        succRef <- nodeRef
                        fingerTable.[0] <- nodeId
                        refTable.[0] <- nodeRef
                    elif (id>succId && nodeId<id && nodeId<succId) then
                        succId <- nodeId
                        succRef <- nodeRef
                        fingerTable.[0] <- nodeId
                        refTable.[0] <- nodeRef
                    elif (id<succId && nodeId>id && nodeId<succId) then
                        succId <- nodeId
                        succRef <- nodeRef
                        fingerTable.[0] <- nodeId
                        refTable.[0] <- nodeRef
                succRef <! Notify(self,id)


        | SendPred senderRef ->
            senderRef <! Stabilize(predId,predRef,1)

        | Notify (nodeRef,nodeId) -> 
            if nodeId <> -1 then
                if (predId = -1 || (predId>id && nodeId>predId && nodeId>id)) then
                    predRef <- nodeRef
                    predId <- nodeId
                elif (predId = -1 || (predId>id && nodeId<predId && nodeId<id)) then
                    predRef <- nodeRef
                    predId <- nodeId
                elif (predId = -1 || (predId<id && nodeId>predId && nodeId<id)) then
                    predRef <- nodeRef
                    predId <- nodeId
            self <! FixFingers


        | StabilizeScheduler ->    
            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(100.0),TimeSpan.FromMilliseconds(200.0), mailbox.Self, Stabilize(id,self,0))

        | NodeScheduler ->
            if requests > 0 then
                self <! KeyFindRequest
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(1000.0), mailbox.Self, NodeScheduler)
            else
                masterRef <! ConvergenceCounter sumHops


        | KeyFindRequest ->
            let rkey = Random().Next(1, pown 2 m)
            self <! FindKeySuccessor(self,rkey,1)


       return! loop()
    }
    loop()

let Master(mailbox: Actor<'a>) =

    let nodes = int (string (Environment.GetCommandLineArgs().[2]))
    let requests = int(string (Environment.GetCommandLineArgs().[3]))
    let mutable savedNode = null
    let nodeArray = Array.zeroCreate(nodes)
    let hopCountArray = Array.zeroCreate(nodes)
    let mutable count = 0
    let mutable sum = 0
    let mutable average = 0.0
    let mutable nodeSet = Set.empty
    let mutable x = 0
    let m = 20


    let rec loop()= actor{
        let! msg = mailbox.Receive()
        match msg with
        | SetParameters _ ->
            while x<nodes do
                let rn = Random().Next(1,pown 2 m)
                if nodeSet.Contains(rn) then
                    x <- x-1
                
                else
                    nodeSet <- nodeSet.Add(rn)
                    let actorSpace = string(rn)
                    let WorkerRef = spawn system (actorSpace) Worker
                    WorkerRef <! InitialiseID(rn,requests,mailbox.Self)
                    nodeArray.[x] <- WorkerRef
                    if x=0 then
                        savedNode <- WorkerRef      
                        savedNode <! Create         
                        
                    else
                        savedNode <! Join(WorkerRef,rn)
                    
                    WorkerRef <! StabilizeScheduler
                x <- x+1

            
            Thread.Sleep(50000)
            for i in [0..(nodes-1)] do
                nodeArray.[i] <! NodeScheduler

        | ConvergenceCounter hopCount ->
            hopCountArray.[count] <- hopCount
            count <- count+1
            if count = nodes then
                for i in hopCountArray do
                    sum <- sum + i
                average <- (float)sum/((float)nodes*(float)requests)
                printfn "Sum = %i Average = %f" sum average
                Environment.Exit(0)
        return! loop()
    }
    loop()

let masterRef = spawn system "MasterSpace" Master

masterRef <! SetParameters "Let's Start!!"

Console.ReadLine() |> ignore