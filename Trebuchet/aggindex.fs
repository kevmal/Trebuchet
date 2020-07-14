namespace Trebuchet
open System
open Util
open Trebuchet.Collections
open System.Runtime.CompilerServices
open System.Numerics
(*
#load @"E:\profile\fsi\.\packagegrp\BA5EA79B5A9288BB73E232DD3FC7E64D075248477E4C7D4B2224ADF99ED2477F\.paket\load\main.group.fsx"
*)
type IChunked<'a> =
    abstract member Count: i: int -> int
    abstract member First: i: int -> 'a
    abstract member Last: i: int -> 'a
    abstract member Max: i: int -> 'a
    abstract member Min: i: int -> 'a
    abstract member Sum: i: int -> 'a
    abstract member ChunkCount: int
    abstract member ChunkIndex: i: int -> int

type IChunkedBuilder<'a> =
    inherit IChunked<'a>
    abstract member SetCount: i: int * count: int -> unit
    abstract member SetFirst: i: int * first: 'a -> unit
    abstract member SetLast: i: int * last: 'a -> unit
    abstract member SetMax: i: int * max: 'a -> unit
    abstract member SetMin: i: int * min: 'a -> unit
    abstract member SetSum: i: int * sum: 'a -> unit



type MemArrayChunked<'a when 'a : unmanaged>(chunkSize, count : int MemArray, first : 'a MemArray, last: 'a MemArray, mx: 'a MemArray, mn: 'a MemArray, sum : 'a MemArray) = 
    new(namer, chunkSize, length : int) = 
        let inline z name = new MemArray<_>(namer name, length)
        MemArrayChunked<'a>(chunkSize,z("count"),z("first"),z("last"),z("max"),z("min"),z("sum"))
    interface IChunkedBuilder<'a> with
        member this.ChunkIndex(i: int): int = chunkSize / i
        member this.ChunkCount: int = first.Length
        member this.First(i: int): 'a = first.[i]
        member this.Last(i: int): 'a = last.[i]
        member this.Max(i: int): 'a = mx.[i]
        member this.Sum(i: int): 'a = sum.[i]
        member this.Min(i: int): 'a = mn.[i]
        member this.SetCount(i: int, countv: int): unit = count.Set(i, countv)
        member this.SetFirst(i: int, firstv: 'a): unit = first.Set(i, firstv)
        member this.SetLast(i: int, lastv: 'a): unit = last.Set(i, lastv)
        member this.SetMax(i: int, max: 'a): unit = mx.Set(i, max)
        member this.SetSum(i: int, sumv: 'a): unit = sum.Set(i, sumv)
        member this.SetMin(i: int, min: 'a): unit = mn.Set(i, min)
        member x.Count(i) = count.[i]

type MemArrayChunkIndex<'a when 'a : unmanaged>(a : 'a MemArray,chunkSizes : int [], chunks : IChunked<'a> []) = 
    member x.Main = a
    member x.ChunkSizes = chunkSizes
    member x.Chunks = chunks

type ArrayChunked<'a>(chunkSize, count : int [], first : 'a [], last: 'a [], mx: 'a [], mn: 'a [], sum : 'a []) = 
    new(chunkSize, length : int) = 
        let inline z() = Array.zeroCreate length
        ArrayChunked<'a>(chunkSize, z(),z(),z(),z(),z(),z())
    member x.SumArray = sum
    interface IChunkedBuilder<'a> with
        member this.ChunkIndex(i: int): int = chunkSize / i
        member this.ChunkCount: int = first.Length
        member this.First(i: int): 'a = first.[i]
        member this.Last(i: int): 'a = last.[i]
        member this.Max(i: int): 'a = mx.[i]
        member this.Sum(i: int): 'a = sum.[i]
        member this.Min(i: int): 'a = mn.[i]
        member this.SetCount(i: int, countv: int): unit = count.[i] <- countv
        member this.SetFirst(i: int, firstv: 'a): unit = first.[i] <- firstv
        member this.SetLast(i: int, lastv: 'a): unit = last.[i] <- lastv
        member this.SetMax(i: int, max: 'a): unit = mx.[i] <- max
        member this.SetSum(i: int, sumv: 'a): unit = sum.[i] <- sumv
        member this.SetMin(i: int, min: 'a): unit = mn.[i] <- min
        member x.Count(i) = count.[i]

type ArrayChunkIndex<'a>(a : 'a [],chunkSizes : int [], chunks : IChunked<'a> []) = 
    member x.Main = a
    member x.ChunkSizes = chunkSizes
    member x.Chunks = chunks

    


module Chunked =
    let inline set (a : IChunkedBuilder<'a>) i (b : IChunked<'a>) j = 
        a.SetSum(i, b.Sum(j))
        a.SetCount(i, b.Count(j))
        a.SetFirst(i, b.First(j))
        a.SetLast(i, b.Last(j))
        a.SetMax(i, b.Max(j))
        a.SetMin(i, b.Min(j))
    let inline add (a : IChunkedBuilder<'a>) i (b : IChunked<'a>) j = 
        a.SetSum(i, b.Sum(j) + a.Sum(i))
        a.SetCount(i, b.Count(j) + a.Count(i))
        a.SetLast(i, b.Last(j))
        a.SetMax(i, max (b.Max(j)) (a.Max(i)))
        a.SetMin(i, min (b.Min(j)) (a.Min(i)))
    let inline chunkMain length get chunkSize (x : IChunkedBuilder<'a>) =
        let mutable i = 0
        let mutable chunk = 0
        let mutable sum = Unchecked.defaultof<_>
        let mutable mn = Unchecked.defaultof<_>
        let mutable mx = Unchecked.defaultof<_>
        let mutable sz = 0
        while i < length do
            let mutable v = get i
            sum <- v
            mn <- v
            mx <- v
            sz <- 1
            x.SetFirst(chunk, v)
            i <- i + 1
            while i < length && sz < chunkSize do 
                v <- get i
                sum <- sum + v
                mn <- min mn v
                mx <- max mx v
                sz <- sz + 1
                i <- i + 1
            x.SetLast(chunk, v)
            x.SetMax(chunk, mx)
            x.SetMin(chunk, mn)
            x.SetCount(chunk, sz)
            x.SetSum(chunk, sum)
            chunk <- chunk + 1
    let inline chunk chunkSize (x : IChunkedBuilder<'a>) (input : IChunked<'a>) = 
    //let chunk chunkSize (x : IChunkedBuilder<double>) (input : IChunked<double>) = 
        let mutable i = 0
        let mutable chunk = 0
        let length = input.ChunkCount
        while i < length do
            set x chunk input i
            i <- i + 1
            let mutable sz = 1
            while i < length && sz < chunkSize do 
                add x chunk input i
                i <- i + 1
                sz <- sz + 1
            chunk <- chunk + 1
    let inline buildIndex getBuilder length get chunking =    
        let cs = chunking |> Seq.toArray
        let chunks = Array.zeroCreate cs.Length
        let len0 = 
            if length % cs.[0] = 0 then 
                length / cs.[0]
            else 
                length / cs.[0] + 1
        chunks.[0] <- getBuilder cs.[0] len0
        if (chunks.[0] :> _ IChunked).Count(0) <> cs.[0] then 
            chunkMain length get cs.[0] chunks.[0]
        let mutable sz = cs.[0]
        for i = 1 to chunks.Length - 1 do 
            let leni = 
                let length = chunks.[i-1].ChunkCount
                if length % cs.[i] = 0 then 
                    length / cs.[i]
                else 
                    length / cs.[i] + 1
            sz <- sz * cs.[i]
            chunks.[i] <- getBuilder sz leni
            if (chunks.[i] :> _ IChunked).Count(0) <> sz then 
                chunk cs.[i] chunks.[i] chunks.[i - 1]
        chunks
    let inline chunkMemArray namer chunking (x : 'a MemArray) =
        buildIndex 
            (fun csz length -> 
                let namer name = 
                    sprintf "%s.%d" name csz |> namer
                MemArrayChunked(namer, csz, length)
            )
            x.Length
            (fun i -> x.[i])
            chunking
    let inline chunkArray chunking (x : 'a []) =
        buildIndex (fun csz length -> ArrayChunked(csz, length)) x.Length (fun i -> x.[i]) chunking

    let inline jumpGeneral length levelCount inc index onValue onChunk =  
        if index >= length then index else
        if onValue index then 
            index 
        elif index + 1 < length && onValue (index + 1) then 
            index + 1
        else 
            let mutable i = index + 1
            let mutable level = 0
            while level < levelCount && i < length && not(onChunk i level) do   
                i <- inc i level
                level <- level + 1
            level <- level - 1
            while level > -1 && i < length do 
                if onChunk i level then 
                    level <- level - 1
                else 
                    i <- inc i level
            while i < length && not(onValue i) do
                i <- i + 1
            i
    let inline jumpArray (x : 'a ArrayChunkIndex) i onValue onChunk = 
        jumpGeneral 
            x.Main.Length
            x.ChunkSizes.Length
            (fun i lvl ->
                let c = i/x.ChunkSizes.[lvl]
                let inc = x.Chunks.[lvl].Count(c)
                c*x.ChunkSizes.[lvl] + inc
            )
            i
            (fun i -> onValue i x.Main)
            (fun i lvl -> onChunk (i/x.ChunkSizes.[lvl]) x.Chunks.[lvl])
    let inline jumpMemArray (x : 'a MemArrayChunkIndex) i onValue onChunk = 
        jumpGeneral 
            x.Main.Length
            x.ChunkSizes.Length
            (fun i lvl ->
                let c = i/x.ChunkSizes.[lvl]
                let inc = x.Chunks.[lvl].Count(c)
                c*x.ChunkSizes.[lvl] + inc
            )
            i
            (fun i -> onValue i x.Main)
            (fun i lvl -> onChunk (i/x.ChunkSizes.[lvl]) x.Chunks.[lvl])
                
    let inline indexArray chunking x = 
        let y = chunkArray chunking x
        let sizes = (1,chunking) ||> Seq.scan (*) |> Seq.skip 1 |> Seq.toArray
        ArrayChunkIndex(x, sizes, y |> Array.map (fun x -> x :> _))
                

    let inline indexMemArray namer chunking x = 
        let y = chunkMemArray namer chunking x
        let sizes = (1,chunking) ||> Seq.scan (*) |> Seq.skip 1 |> Seq.toArray
        MemArrayChunkIndex(x, sizes, y |> Array.map (fun x -> x :> _))
                
    let inline iterGeneral index contin boundry chunkIndex chunkCount moveUp moveUp2 onValue onChunk = 
    //let iterGeneral index contin boundry chunkIndex chunkCount moveUp moveUp2 onValue onChunk = 
        let mutable i : int = index 
        let mutable lvl = -1
        let b = boundry lvl i
        while i < b && contin i do i <- onValue i
        if moveUp i then 
            lvl <- 0
            let mutable newLevel = lvl
            let mutable upb = boundry lvl i
            while contin i && newLevel >= lvl do 
                if newLevel > lvl then 
                    upb <- boundry lvl i
                else
                    upb <- -1
                lvl <- newLevel
                let mutable chunk = chunkIndex lvl i
                while contin i && not ((newLevel <- moveUp2 upb lvl i chunk; newLevel <> lvl)) && onChunk lvl chunk do 
                    i <- i + chunkCount lvl chunk
                    chunk <- chunk + 1
        while contin i do i <- onValue i

    //let inline iterVectorGeneral index contin upto0 upto1 chunkIndex backToIndex onValues onChunks = 
    let iterVectorGeneral index contin upto0 upto1 upto2 chunkIndex backToIndex onValues onChunks = 
        let j = upto0 index
        let mutable i = index
        if j >= index then 
            onValues index j
            i <- j + 1
        if contin i then 
            let mutable lvl = 0
            while contin j && lvl > -1 do 
                let c = chunkIndex lvl i
                let k = upto1 i lvl c
                if k >= c then 
                    i <- backToIndex lvl (k + 1)
                    onChunks lvl c k
                else 
                    lvl <- ~~~k
        if contin i then 
            let k = upto2 i
            if k >= i then 
                onValues i k
            

    //let inline iterArraySliceVector (x : 'a ArrayChunkIndex) startIndex endIndex onValues onChunks = 
    let iterArraySliceVector (x : 'a ArrayChunkIndex) startIndex endIndex onValues onChunks = 
        let inline boundry lvl i = 
            let l = lvl + 1
            let sz = x.ChunkSizes.[l]
            if i % sz = 0 then 
                i
            else 
                (i / sz + 1) * sz
        let inline chunkIndex lvl i = i / x.ChunkSizes.[lvl]
        iterVectorGeneral 
            startIndex 
            (fun i -> i <= endIndex) // continue
            (fun i -> //upto0
                let b = (boundry -1 i)
                if b = i then 
                    -1
                else 
                    min (b - 1) endIndex
            )
            (fun i lvl c -> //upto1
                if lvl = x.Chunks.Length - 1 then 
                    c + (endIndex - i + 1) / x.ChunkSizes.[lvl] - 1
                else 
                    let b = boundry lvl i
                    if b > endIndex then 
                        let ind = c + (endIndex - i + 1) / x.ChunkSizes.[lvl] - 1
                        if ind < 0 then
                            ~~~(lvl - 1)
                        else ind
                    else 
                        if (endIndex - b + 1) / x.ChunkSizes.[lvl + 1] > 0 then 
                            if i = b then 
                                ~~~(lvl + 1)
                            else
                                c + (b - 1 - i + 1) / x.ChunkSizes.[lvl] - 1
                        else
                            c + (endIndex - i + 1) / x.ChunkSizes.[lvl] - 1
            )
            (fun i -> //upto2
                endIndex
            )
            chunkIndex
            (fun lvl chunk -> chunk*x.ChunkSizes.[lvl] ) //backToIndex
            (fun si ei -> //onValues
                onValues x.Main si ei
            )
            (fun lvl si ei -> //onChunks
                onChunks x.Chunks.[lvl] si ei
            )
        
        
    let inline iterArraySlice (x : 'a ArrayChunkIndex) startIndex endIndex onValue onChunk = 
    //let iterArraySlice (x : double ArrayChunkIndex) startIndex endIndex onValue onChunk = 
        iterGeneral 
            startIndex 
            (fun i -> i <= endIndex)
            (fun lvl i -> 
                if lvl = x.ChunkSizes.Length - 1 then 
                    Int32.MaxValue
                else 
                    let l = lvl + 1
                    let sz = x.ChunkSizes.[l]
                    if i % sz = 0 then 
                        i
                    else 
                        (i / sz + 1) * sz
            )
            (fun lvl i -> i / x.ChunkSizes.[lvl])
            (fun lvl chunk -> x.Chunks.[lvl].Count chunk)
            (fun i -> 
                let sz0 = x.ChunkSizes.[0]
                i + sz0 <= endIndex
            )
            (fun upb lvl i chunk -> 
                if (1 + chunk)*x.ChunkSizes.[lvl] >= endIndex then 
                    lvl - 1
                elif upb <> i || lvl = x.ChunkSizes.Length - 1 then 
                    lvl
                else 
                    let sz0 = x.ChunkSizes.[lvl + 1]
                    if i + sz0 <= endIndex then 
                        lvl + 1
                    else lvl
            )
            (fun i -> 
                onValue (x.Main.[i])
                i + 1
            )
            (fun lvl chunk -> 
                onChunk x.Chunks.[lvl] chunk
                true 
            )

    let inline iterMemArraySlice (x : 'a MemArrayChunkIndex) startIndex endIndex onValue onChunk = 
    //let iterArraySlice (x : double ArrayChunkIndex) startIndex endIndex onValue onChunk = 
        iterGeneral 
            startIndex 
            (fun i -> i <= endIndex)
            (fun lvl i -> 
                if lvl = x.ChunkSizes.Length - 1 then 
                    Int32.MaxValue
                else 
                    let l = lvl + 1
                    let sz = x.ChunkSizes.[l]
                    if i % sz = 0 then 
                        i
                    else 
                        (i / sz + 1) * sz
            )
            (fun lvl i -> i / x.ChunkSizes.[lvl])
            (fun lvl chunk -> x.Chunks.[lvl].Count chunk)
            (fun i -> 
                let sz0 = x.ChunkSizes.[0]
                i + sz0 <= endIndex
            )
            (fun upb lvl i chunk -> 
                if (1 + chunk)*x.ChunkSizes.[lvl] >= endIndex then 
                    lvl - 1
                elif upb <> i || lvl = x.ChunkSizes.Length - 1 then 
                    lvl
                else 
                    let sz0 = x.ChunkSizes.[lvl + 1]
                    if i + sz0 <= endIndex then 
                        lvl + 1
                    else lvl
            )
            (fun i -> 
                onValue (x.Main.[i])
                i + 1
            )
            (fun lvl chunk -> 
                onChunk x.Chunks.[lvl] chunk
                true 
            )
