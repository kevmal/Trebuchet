namespace Trebuchet
open System
open Util
open Trebuchet.Collections

module DataIndex = 
    open System.Runtime.CompilerServices


    let inline timeKey (startTime : DateTime) (r1 : int64) (x : DateTime) = ((x.Ticks - startTime.Ticks)/ r1) |> int

    type ITimeIndex = 
        abstract member StartTime : DateTime 
        abstract member EndTime : DateTime 
        abstract member Range : TimeSpan
        abstract member FindSampleIndex: time : DateTime -> int
    type ITimeChunkIndex = 
        inherit ITimeIndex
        abstract member Interval : int64

    module TimeChunk =
        let inline buildTimeChunkIndex (length : int) (indexArraySize : int) (getKey : int -> DateTime) make (get : _ -> int -> int) (set : _ -> int -> int -> unit) =
            let startTime = if length > 0 then getKey 0  else DateTime.MaxValue
            let endTime = if length > 0 then getKey (length - 1) else DateTime.MaxValue
            let range = endTime - startTime
            let ml1 = indexArraySize //keys.Length*4
            let r1 = range.Ticks / int64 ml1 |> max 1L
            let last = timeKey startTime r1 endTime
            let a = make (last + 2)
            let alen = last + 2
            do
                for j = 0 to length - 1 do
                    set a (timeKey startTime r1 (getKey j)) j 
                for i = 1 to alen - 1 do 
                    if get a i < get a (i - 1) then 
                        set a  i (get a (i - 1))
            struct(startTime,endTime,r1,alen,a)

        let inline findSampleIndex get getKey keyLength startTime r1 alen a (t: DateTime): int = 
            let ind = timeKey startTime r1 t
            if ind <= 0 then 
                if t.Ticks < startTime.Ticks then 
                    -1
                else 
                    0
            elif ind >= alen then 
                keyLength - 1
            else 
                let i1 = get a (ind - 1)
                let i2 = get a ind
                if i1 = i2 then 
                    i1
                else 
                    binarySearchGeneral (fun (a : DateTime) (b : DateTime) -> a.Ticks > b.Ticks) getKey i1 i2 t
        let inline findSampleIndexAA r1 startTime (keys : DateTime []) (a : int []) t =
            findSampleIndex (fun (a : _ []) i -> a.[i]) (fun i -> keys.[i]) keys.Length startTime r1 a.Length a t
        let inline findSampleIndexMM r1 startTime (keys : DateTime MemArray) (a : int MemArray) t =
            findSampleIndex (fun (a : _ MemArray) i -> a.[i]) (fun i -> keys.[i]) keys.Length startTime r1 a.Length a t
        let inline findSampleIndexAM r1 startTime (keys : DateTime []) (a : int MemArray) t =
            findSampleIndex (fun (a : _ MemArray) i -> a.[i]) (fun i -> keys.[i]) keys.Length startTime r1 a.Length a t

        let inline buildTimeChunkIndexAA indexArraySize (keys : DateTime []) =
            buildTimeChunkIndex keys.Length indexArraySize (fun i -> keys.[i]) (Array.zeroCreate) (fun a i -> a.[i]) (fun a i j -> a.[i] <- j)
        let inline buildTimeChunkIndexAM indexArraySize (keys : DateTime []) =
            buildTimeChunkIndex 
                keys.Length 
                indexArraySize 
                (fun i -> keys.[i]) 
                (fun i -> new MemArray<_>(i)) (fun a i -> a.[i]) (fun a i j -> a.Set(i, j))
        let inline buildTimeChunkIndexMM indexArraySize (keys : DateTime MemArray) =
            buildTimeChunkIndex 
                keys.Length 
                indexArraySize 
                (fun i -> keys.[i]) 
                (fun i -> new MemArray<_>(i)) (fun a i -> a.[i]) (fun a i j -> a.Set(i, j))        
    open TimeChunk
    [<Sealed>]
    type TimeChunkIndexMem(keys : DateTime [], indexArraySize : int) = 
        let (struct(startTime,endTime,r1,alen,a)) = 
            buildTimeChunkIndex 
                keys.Length 
                indexArraySize 
                (fun i -> keys.[i]) 
                (fun i -> new MemArray<_>(i)) (fun a i -> a.[i]) (fun a i j -> a.Set(i, j))
        let range = endTime - startTime
        new(keys) = TimeChunkIndexMem(keys, keys.Length * 4) 
        member x.Range = range
        member x.StartTime = startTime
        member x.EndTime = endTime
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member x.FindSampleIndex(t : DateTime) = 
            findSampleIndex (fun (a : _ MemArray) i -> a.[i]) (fun i -> keys.[i]) keys.Length startTime r1 alen a t
        member x.Save(filename) = 
            use ma = new MemArray<int>(filename, a.Length)
            for i = 0 to a.Length - 1 do 
                ma.Set(i, a.[i])
        interface ITimeIndex with
            member this.EndTime: DateTime = endTime
            member this.Range: TimeSpan = range
            member this.StartTime: DateTime = startTime
            [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
            member x.FindSampleIndex(t : DateTime) = x.FindSampleIndex(t)
    [<Sealed>]
    type TimeChunkIndex(keys : DateTime [], indexArraySize : int) = 
        let (struct(startTime,endTime,r1,alen,a)) = 
            buildTimeChunkIndex keys.Length indexArraySize (fun i -> keys.[i]) (Array.zeroCreate) (fun a i -> a.[i]) (fun a i j -> a.[i] <- j)
        let range = endTime - startTime
        new(keys) = TimeChunkIndex(keys, keys.Length * 4) 
        member x.Range = range
        member x.StartTime = startTime
        member x.EndTime = endTime
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member x.FindSampleIndex(t : DateTime) = 
            findSampleIndex (fun (a : _ []) i -> a.[i]) (fun i -> keys.[i]) keys.Length startTime r1 alen a t
        member x.Save(filename) = 
            use ma = new MemArray<int>(filename, a.Length)
            for i = 0 to a.Length - 1 do 
                ma.Set(i, a.[i])
        interface ITimeIndex with
            member this.EndTime: DateTime = endTime
            member this.Range: TimeSpan = range
            member this.StartTime: DateTime = startTime
            [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
            member x.FindSampleIndex(t : DateTime) = x.FindSampleIndex(t)

    type JumpIndex<'v> = 
        {
            FirstValue : 'v
            LastValue : 'v
            Chunks : 'v []
            ChunkSize : int 
            Parent : JumpIndex<'v> option
        }
        member x.ChunkCount = x.Chunks.Length
    module JumpIndex  =
        let createTrivial (values : 'v []) =   
            {
                FirstValue = values.[0]
                LastValue = values.[values.Length - 1]
                Chunks = values
                ChunkSize = 1
                Parent = None
            }
        let createLevel chunkSize (x : JumpIndex<'v>) = 
            let len = 
                if x.ChunkCount % chunkSize = 0 then 
                    x.ChunkCount / chunkSize
                else
                    x.ChunkCount / chunkSize + 1
            let chunks = Array.init len (fun i -> if i = len - 1 then x.LastValue else x.Chunks.[i*chunkSize + chunkSize - 1])
            {
                FirstValue = x.FirstValue
                LastValue = x.LastValue
                Chunks = chunks
                ChunkSize = chunkSize
                Parent = None
            }
            
        let rec addLevel chunkSize (x : JumpIndex<'v>) = 
            match x.Parent with 
            | None -> 
                {x with Parent = Some(createLevel chunkSize x)}
            | Some p -> addLevel chunkSize p

        let rec topLevelChunkCount (x : JumpIndex<'v>) = 
            match x.Parent with 
            | None -> x.ChunkCount
            | Some p -> topLevelChunkCount p
            
        let create n chunkSize (a : 'v []) = 
            let b = createTrivial a
            let l1 = createLevel n b
            let rec loop (x : 'v JumpIndex) = 
                if x.ChunkCount <= n then 
                    Some x
                else 
                    let next = createLevel chunkSize x
                    Some 
                        { x with 
                            Parent = loop next
                        }
            {b with Parent = loop l1}
        let itemIndexToChunkIndex i (x : JumpIndex<'a>)  = i / x.ChunkSize
        let chunkIndexToItemIndex i (x : JumpIndex<'a>)  = i * x.ChunkSize
            
        let jump index right (x : JumpIndex<'a>) = 
            let rec jump chunkIndex (x : JumpIndex<'a>) = 
                let rec loop i =
                    if i >= x.ChunkCount then 
                        -1 
                    elif right x.Chunks.[i] then 
                        loop (i + 1)
                    else 
                        i
                let search l rr = 
                    let r = min rr (x.ChunkCount - 1)
                    if r - l >= 16 then 
                        let mutable si = l
                        let mutable ei = r
                        while si < ei do
                            let m = ((ei - si) >>> 1) + si
                            if right x.Chunks.[m] then
                                si <- m + 1
                            else
                                ei <- m - 1
                        if right x.Chunks.[si] then
                            si + 1
                        else
                            si
                    else 
                        loop l
                match x.Parent with 
                | None -> search chunkIndex (x.ChunkCount - 1)
                | Some p -> 
                    let rec loop chunkIndex = 
                        let pi = p |> itemIndexToChunkIndex(chunkIndex)
                        if not (right p.Chunks.[pi]) then 
                            search chunkIndex (chunkIndexToItemIndex (pi + 1) p)
                        else 
                            let pi2 = jump pi p
                            if pi2 = -1 then 
                                -1
                            else 
                                let i = chunkIndexToItemIndex pi2 p
                                search i (chunkIndexToItemIndex (pi2 + 1) p)
                    loop chunkIndex         
            if right x.LastValue then 
                x.ChunkCount - 1
            else
                (jump index x) - 1
    [<Struct>]
    type JumpCursor =
        val mutable index : int
        val JumpIndex : JumpIndex<DateTime>
        new(ji) = {index = 0; JumpIndex = ji}
        member x.Next(k : DateTime) = 
            if not(x.index = x.JumpIndex.ChunkCount - 1) then 
                x.index <-  x.JumpIndex |> JumpIndex.jump x.index (fun x -> x.Ticks <= k.Ticks) 
                            
            

    type MinMaxValues<'v> = 
        {
            Min : 'v
            Max : 'v
            ChunkMax : 'v []
            ChunkMin : 'v []
            ChunkSize : int 
            Parent : MinMaxValues<'v> option
        }
        member x.ChunkCount = x.ChunkMin.Length
    module MinMaxValues  =
        let createTrivial (mins : 'v []) (maxs : 'v []) =   
            let mutable mx = maxs.[0]
            let mutable mn = mins.[0]
            for i = 0 to maxs.Length - 1 do 
                mx <- max mx maxs.[i]
                mn <- min mn mins.[i]
            {
                Min = mn
                Max = mx 
                ChunkMax = maxs
                ChunkMin = mins
                ChunkSize = 1
                Parent = None
            }
        let createLevel chunkSize (x : MinMaxValues<'v>) = 
            let mnValues = x.ChunkMin
            let mxValues = x.ChunkMax
            let len = 
                if mnValues.Length % chunkSize = 0 then 
                    mnValues.Length / chunkSize
                else
                    mnValues.Length / chunkSize + 1
            let chunkMax = Array.zeroCreate len
            let chunkMin = Array.zeroCreate len
            do
                let mutable j = -1
                for i = 0 to mnValues.Length - 1 do 
                    if i % chunkSize = 0 then 
                        j <- j + 1
                        chunkMax.[j] <- mxValues.[i]
                        chunkMin.[j] <- mnValues.[i]
                    else 
                        chunkMax.[j] <- max mxValues.[i] chunkMax.[j]
                        chunkMin.[j] <- min mnValues.[i] chunkMin.[j]
            {
                Min = x.Min
                Max = x.Max
                ChunkMax = chunkMax
                ChunkMin = chunkMin
                ChunkSize = chunkSize
                Parent = None
            }
            
        let rec addLevel chunkSize (x : MinMaxValues<'v>) = 
            match x.Parent with 
            | None -> 
                {x with Parent = Some(createLevel chunkSize x)}
            | Some p -> addLevel chunkSize p

        let rec topLevelChunkCount (x : MinMaxValues<'v>) = 
            match x.Parent with 
            | None -> x.ChunkMin.Length
            | Some p -> topLevelChunkCount p
            
        let create n chunkSize (a : 'v []) = 
            let b = createTrivial a a
            let l1 = createLevel n b
            let rec loop (x : 'v MinMaxValues) = 
                if x.ChunkCount <= n then 
                    Some x
                else 
                    let next = createLevel chunkSize x
                    Some 
                        { x with 
                            Parent = loop next
                        }
            {b with Parent = loop l1}
        let itemIndexToChunkIndex i (x : MinMaxValues<'a>)  = i / x.ChunkSize
        let chunkIndexToItemIndex i (x : MinMaxValues<'a>)  = i * x.ChunkSize
            
        let rec nextMatch chunkIndex f (x : MinMaxValues<'a>) = 
            match x.Parent with 
            | None ->
                let rec loop i =
                    if i >= x.ChunkCount then 
                        -1 
                    elif f x.ChunkMin.[i] x.ChunkMax.[i] then 
                        i
                    else 
                        loop (i + 1)
                loop chunkIndex
            | Some p -> 
                let rec loop chunkIndex = 
                    let pi = p |> itemIndexToChunkIndex(chunkIndex)
                    let pi2 = p |> nextMatch pi f
                    if pi = pi2 then 
                        //check remainder of parent chunk at this level
                        let nextParentChunk = p |> chunkIndexToItemIndex(pi + 1) |> min (x.ChunkCount - 1)
                        rest chunkIndex nextParentChunk
                    elif pi2 = -1 then 
                        -1
                    else 
                        let nextParentChunk = p |> chunkIndexToItemIndex(pi2 + 1) |> min (x.ChunkCount - 1)
                        let i = p |> chunkIndexToItemIndex(pi2)
                        rest i nextParentChunk
                and rest i nextParentChunk = 
                    if i = nextParentChunk then 
                        loop i
                    elif i >= x.ChunkCount then 
                        -1 
                    elif f x.ChunkMin.[i] x.ChunkMax.[i] then 
                        i
                    else 
                        rest (i + 1) nextParentChunk
                loop chunkIndex                
            