module Trebuchet.Util 
open System
open System.Threading




/// Binary search where returned index is of the value less or equal to the query. If no value exists -1 is returned.
let inline binarySearchGeneral (>.) get (startIndex : int) (endIndex : int) v = 
    let mutable si = startIndex
    let mutable ei = endIndex
    while si < ei do
        let m = ((ei - si) >>> 1) + si
        if get m >. v then
            ei <- m - 1
        else
            si <- m + 1
    if get si >. v then 
        si - 1 
    else si


/// Binary search where returned index is of the value less or equal to the query. If no value exists -1 is returned.
let inline binarySearch (>.) (a : 'v []) (startIndex : int) (endIndex : int) v = 
    let mutable si = startIndex
    let mutable ei = endIndex
    while si < ei do
        let m = ((ei - si) >>> 1) + si
        if a.[m] >. v then
            ei <- m - 1
        else
            si <- m + 1
    if a.[si] >. v then 
        si - 1 
    else si



type ProgressSummary = 
    {
        TimeStamp : DateTime
        Elapsed : TimeSpan
        CurrentStep : int64
        TotalSteps : int64
        Eta : TimeSpan
        LastLogValue : obj
        LastLogTime : TimeSpan
        ProgressId : int
    }
    member x.SampledPerTick = 
        decimal x.CurrentStep / decimal (x.Elapsed.Ticks + 1L)
    member x.SamplesPer(ts : TimeSpan) = x.SampledPerTick * decimal ts.Ticks
    member x.SamplesPerSecond = x.SamplesPer(TimeSpan.FromSeconds 1.0)
    member x.SamplesPerMinute = x.SamplesPer(TimeSpan.FromMinutes 1.0)
    member x.SamplesPerHour = x.SamplesPer(TimeSpan.FromHours 1.0)

type ProgressReference(p : Progress) = 
    member x.CurrentStep = p.CurrentStep
    member val Active = true
    
and Progress() = 
    static let mutable idCounter = 0
    let sw = Diagnostics.Stopwatch()
    let pid = Threading.Interlocked.Increment &idCounter
    let totalSteps = ref 0L
    let currentStep = ref 0L
    let mutable lastRefTaken = DateTime.MinValue
    let mutable lastLogObj = null
    let mutable lastLogTime = TimeSpan.Zero
    let refs = ResizeArray()
    let lck = SpinLock()
    let logobjs = ResizeArray()
    let snapLock = SpinLock()
    let snapTimes = ResizeArray()
    let snapCurrent = ResizeArray()
    let snapTotal = ResizeArray()
    member val SnapshotCapacity = 4096
    member val SnapshotTargetSize = 1024
    member x.SnapshotResize() = 
        if snapTimes.Count >= x.SnapshotCapacity then 
            let mutable taken = Unchecked.defaultof<_>
            if not snapLock.IsHeldByCurrentThread then snapLock.Enter(&taken) 
            try 
                let toRemove = snapTimes.Count - x.SnapshotTargetSize
                logobjs.RemoveRange(0, toRemove)
                snapTimes.RemoveRange(0, toRemove)
                snapCurrent.RemoveRange(0, toRemove)
                snapTotal.RemoveRange(0, toRemove)
            finally 
                if taken then snapLock.Exit()
    member x.Snapshot(item : obj) = 
        let mutable taken = Unchecked.defaultof<_>
        if not snapLock.IsHeldByCurrentThread then snapLock.Enter(&taken) 
        try
            let ts = Threading.Interlocked.Read totalSteps
            let cs = Threading.Interlocked.Read currentStep
            let t = sw.Elapsed
            snapTimes.Add t
            snapCurrent.Add cs
            snapTotal.Add ts
            logobjs.Add item
            if not(isNull item) then 
                lastLogObj <- item
                lastLogTime <- t
            if snapTimes.Count >= x.SnapshotCapacity then 
                x.SnapshotResize()
        finally
            if taken then snapLock.Exit()
    member x.Snapshot() = x.Snapshot(null)
    member x.Id = pid
    member x.TotalSteps = totalSteps
    member x.CurrentStep = currentStep
    member val AutoStart = true with get,set
    member x.Ref() = 
        let pr = ProgressReference(x)
        let r = WeakReference<ProgressReference>(pr)
        let mutable taken = false
        lck.Enter(&taken) 
        try
            refs.Add(r)            
            lastRefTaken <- DateTime.Now
            if x.AutoStart && not sw.IsRunning then sw.Start()
            pr
        finally
            if taken then lck.Exit()
    member x.RefreshReferences() = 
        let mutable taken = false
        lck.Enter(&taken) 
        try
        for i = refs.Count - 1 downto 0 do 
            match refs.[i].TryGetTarget() with 
            | true,o when not o.Active -> refs.RemoveAt(i)
            | false,_ -> refs.RemoveAt(i)
            | _ -> ()
        finally
            if taken then lck.Exit()
    member x.ReferenceCount = refs.Count    
    member x.LastReferenceTaken = lastRefTaken    
    member x.Summary() = 
        let mutable taken = false
        if not snapLock.IsHeldByCurrentThread then snapLock.Enter(&taken) 
        try 
            x.Snapshot()
            let ts = snapTotal.[snapTotal.Count - 1]
            let cs = snapCurrent.[snapCurrent.Count - 1]
            let elapsed = snapTimes.[snapTimes.Count - 1]
            let eta = 
                if snapTimes.Count = 1 || ts = 0L || elapsed.Ticks = 0L || cs = 0L then 
                    TimeSpan.MaxValue
                else
                    let samplesPerTick = decimal cs / decimal (elapsed.Ticks)
                    let remainder = decimal (ts - cs)
                    remainder / samplesPerTick |> int64 |> TimeSpan
            {
                TimeStamp = DateTime.Now
                ProgressId = x.Id
                Elapsed = snapTimes.[snapTimes.Count - 1]
                CurrentStep = snapCurrent.[snapCurrent.Count - 1]
                TotalSteps = snapTotal.[snapTotal.Count - 1]
                Eta = eta
                LastLogValue = lastLogObj
                LastLogTime = lastLogTime
            }
        finally
            if taken then snapLock.Exit()


module Progress = 
    open System.Collections.Generic

    let lck = obj()
    let monitored = ResizeArray<Progress>() 
    //let mutable elapsed = 10
    //let mutable eta = 10
    //let mutable samplesPerSecond = 10
    //let mutable samplesPerMinute = 10
    let mutable progressId = 3
    let mutable lastNonNullLog = 20
    let mutable printer = None
    let mutable snapShotInterval = TimeSpan.FromSeconds 1.0
    let mutable printInterval = TimeSpan.FromSeconds 5.0
    let mutable cleanInterval = TimeSpan.FromSeconds 240.0
    let mutable printRatioInterval = 0.1
    let mutable minPrintInterval = TimeSpan.FromSeconds 1.0
    let defaultPrinter (x : ProgressSummary) = 
        let sb = Text.StringBuilder()
        sb.Append(x.ProgressId.ToString().PadRight(4)) |> ignore
        if x.TotalSteps = 0L then 
            sb.Append(x.Elapsed.ToString().PadRight(10)) |> ignore
            sb.Append(x.CurrentStep.ToString().PadLeft(10)) |> ignore
            if x.SamplesPerMinute > 100000m then 
                sb.Append(x.SamplesPerSecond.ToString(".00").PadLeft(16)) |> ignore
                sb.Append("SPS") |> ignore
            else 
                sb.Append(x.SamplesPerMinute.ToString(".00").PadLeft(16)) |> ignore
                sb.Append("SPM") |> ignore
            match x.LastLogValue with 
            | null -> ()
            | v -> 
                sb.Append(sprintf " Item (%O): %O" x.LastLogTime v) |> ignore
        else
            sb.Append(x.Elapsed.ToString().PadRight(10)) |> ignore
            sb.Append(x.CurrentStep.ToString().PadLeft(10)) |> ignore
            sb.Append "/" |> ignore
            sb.Append(x.TotalSteps.ToString().PadRight(10)) |> ignore
            let pc = decimal x.CurrentStep / decimal x.TotalSteps 
            sb.Append(pc.ToString(".00%").PadLeft(10)) |> ignore
            sb.Append(x.SamplesPerMinute.ToString(".00").PadLeft(10)) |> ignore
            sb.Append(" SPM  ETA: ") |> ignore
            sb.Append(x.Eta.ToString().PadRight(10)) |> ignore
            match x.LastLogValue with 
            | null -> ()
            | v -> 
                sb.Append(sprintf " Item (%O): %O" x.LastLogTime v) |> ignore
        printfn "%s" (sb.ToString())
    let print x = 
        match printer with 
        | None -> defaultPrinter x
        | Some p -> p x
    let mutable printerActive = 0
    let mutable printerTask = None
    let unwatch (p : Progress) = lock(lck)(fun _ -> monitored |> Seq.tryFindIndex (fun x -> x.Id = p.Id) |> Option.iter (monitored.RemoveAt))
    let makePrinter() = 
        lock(lck) 
            (fun _ -> 
                match printerTask with 
                | None -> 
                    Threading.Interlocked.Increment &printerActive |> ignore
                    let task = 
                        async {
                            let summaries = Dictionary<int, ProgressSummary>()
                            lock(lck) (fun _ -> monitored.ToArray() ) |> Array.iter (fun (x : Progress) -> summaries.Add(x.Id, x.Summary()))
                            let mutable last = DateTime.Now
                            let mutable running = true
                            while printerActive > 0 && running do 
                                do! Async.Sleep (int snapShotInterval.TotalMilliseconds)
                                let clean = (DateTime.Now - last).Ticks > cleanInterval.Ticks 
                                last <- DateTime.Now
                                let m = lock(lck) (fun _ -> monitored.ToArray())
                                for x in m do 
                                    match summaries.TryGetValue(x.Id) with 
                                    | true, s -> 
                                        //printfn "%A" (DateTime.Now - s.TimeStamp, printInterval)
                                        if s.TotalSteps > 0L && s.CurrentStep = s.TotalSteps then 
                                            if clean then 
                                                unwatch x
                                        elif (DateTime.Now - s.TimeStamp).Ticks > printInterval.Ticks then 
                                            let s = x.Summary()
                                            summaries.[x.Id] <- s
                                            if printerActive > 0 then 
                                                print s
                                        elif s.TotalSteps = 0L || printRatioInterval = 0.0 || (DateTime.Now - s.TimeStamp).Ticks < minPrintInterval.Ticks then 
                                            x.Snapshot()
                                        else
                                            let s2 = x.Summary()
                                            if (decimal (s2.CurrentStep - s.CurrentStep) / decimal s2.TotalSteps) >= decimal printRatioInterval then 
                                                summaries.[x.Id] <- s2
                                                if printerActive > 0 then 
                                                    print s2
                                            if s2.CurrentStep = s.CurrentStep && clean then 
                                                x.RefreshReferences()
                                                if x.ReferenceCount = 0 then 
                                                    unwatch x
                                    | _ -> 
                                        let s = x.Summary()
                                        summaries.[x.Id] <- s
                                if clean then 
                                    lock(lck) (fun _ -> 
                                        if monitored.Count = 0 then
                                            running <- false
                                            printerTask <- None)
                        }
                    printerTask <- task |> Async.StartAsTask |> Some
                | Some _ -> ()
            )

    let watch (p : Progress) = 
        lock(lck) (fun _ -> if monitored |> Seq.exists (fun i -> i.Id = p.Id) then () else monitored.Add p)
        makePrinter()
        p
    let inline step (p : ProgressReference) = Threading.Interlocked.Increment(p.CurrentStep) |> ignore
    let dump() =
        let xs = lock(lck)(fun _ -> monitored.ToArray()) |> Array.map (fun x -> x.Summary())
        lock(lck) (fun _ -> xs |> Array.iter print)
    let track f = 
        let p = Progress()
        p |> watch |> ignore
        let pr = p.Ref()
        try 
            f pr
        finally
            Threading.Interlocked.Decrement &printerActive |> ignore
            print (p.Summary())
    let inline trackTotal total f = 
        let p = Progress()
        p.TotalSteps := int64 total
        p |> watch |> ignore
        let pr = p.Ref()
        try 
            f pr
        finally
            Threading.Interlocked.Decrement &printerActive |> ignore
            print (p.Summary())
module Array = 
    let inline mapWithProgress f a = 
        let p = Progress()
        p.TotalSteps :=  Array.length a |> int64
        let pr = p.Ref()
        Progress.watch p |> ignore
        try 
            a 
            |> Array.map 
                (fun x ->
                    let r = f x
                    Progress.step pr
                    r
                )
        finally 
            Threading.Interlocked.Decrement &Progress.printerActive |> ignore
            Progress.print (p.Summary())
            
    let inline iterWithProgress f a = 
        let p = Progress()
        p.TotalSteps :=  Array.length a |> int64
        let pr = p.Ref()
        Progress.watch p |> ignore
        try
            a 
            |> Array.iter
                (fun x ->
                    f x
                    Progress.step pr
                )
        finally 
            Threading.Interlocked.Decrement &Progress.printerActive |> ignore
            Progress.print (p.Summary())

