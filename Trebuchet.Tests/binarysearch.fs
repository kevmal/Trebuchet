module binarysearch



open Xunit
open Trebuchet.Util
open System
open Trebuchet.DataStructures
open System.Collections.Generic
open Swensen.Unquote
open Trebuchet.DataIndex
open Trebuchet
open System.Numerics


let someTests = """
1 2 3 6 8 9
10 5
2 1
1 0
0 -1
9 5
8 4
7 3
6 3
4 2
3 2
5 2
-1 -1
"""

[<Fact>]
let ``general checks``() = 
    let lines = someTests.Split('\n')  |> Array.filter (String.IsNullOrWhiteSpace >> not)
    let l = lines.[0] .Split ' ' |> Array.filter (String.IsNullOrWhiteSpace >> not) |> Array.map int 
    for i in lines.[1 .. ] do 
        let a = i.Split(' ') |> Array.filter (String.IsNullOrWhiteSpace >> not) |> Array.map int
        test <@ binarySearch (>) l 0 (l.Length - 1) a.[0] = a.[1] @>
    



[<Fact>]
let ``time chunk index``() = 
    let times = [|23 .. 5 .. 32423|] |> Array.map (int64 >> DateTime)
    let ix = TimeChunkIndex(times)
    test <@ (times |> Array.map ix.FindSampleIndex) = [|0 .. times.Length - 1|] @>
    test <@ (times |> Array.map (fun x -> x.AddTicks(-1L)) |> Array.map ix.FindSampleIndex) = [|-1 .. times.Length - 2|] @>
    test <@ (times |> Array.map (fun x -> x.AddTicks(1L)) |> Array.map ix.FindSampleIndex) = [|0 .. times.Length - 1|] @>
    test <@ ix.FindSampleIndex(DateTime(3243824L)) = times.Length - 1@>



[<Fact>]
let ``jump index simple``() = 
    let times = [| 0 .. 5 |] |> Array.map (int64 >> DateTime)
    let ix = JumpIndex.create 10 2 times
    let mutable c = JumpCursor(ix)
    let mutable t = DateTime(0L)
    while t.Ticks < 8L do 
        c.Next(t)
        let i = c.index
        let j = binarySearch (>) times 0 (times.Length - 1) t// |> Array.findIndex (fun c-> c)
        test <@ i = j @>
        t <- t.AddTicks(int64 1)
    

[<Fact>]
let ``jump using timechunk``() = 
    let times = [|23 .. 5 .. 32423|] |> Array.map (int64 >> DateTime)
    let cix = TimeChunkIndex(times)
    let ix = JumpIndex.create 4 2 times
    for tinc in [1;4;5;7;15;200;1000;10000;20000] do 
        let mutable t = DateTime(0L)
        while t.Ticks < 50000L do 
            while t < times.[0] do t <- t.AddTicks(int64 tinc)
            let i = cix.FindSampleIndex(t)
            let j = binarySearch (>) times 0 (times.Length - 1) t// |> Array.findIndex (fun c-> c)
            let ticks=  t.Ticks
            //test <@ (ticks,times.[i].Ticks,i) = (ticks, times.[j].Ticks, j) @> // cix.FindSampleIndex(t) @>
            //test <@ (ticks,i) = (ticks, j) @> // cix.FindSampleIndex(t) @>
            Assert.Equal(j,i)
            t <- t.AddTicks(int64 tinc)
[<Fact>]
let ``jump index 1``() = 
    let times = [|23 .. 5 .. 32423|] |> Array.map (int64 >> DateTime)
    //let cix = TimeChunkIndex(times)
    let ix = JumpIndex.create 4 2 times
    for tinc in [1;4;5;7;15;200;1000;10000;20000] do 
        let mutable c = JumpCursor(ix)
        let mutable t = DateTime(0L)
        while t.Ticks < 50000L do 
            while t < times.[0] do t <- t.AddTicks(int64 tinc)
            c.index <- 0
            c.Next(t)
            let i = c.index
            let j = binarySearch (>) times 0 (times.Length - 1) t// |> Array.findIndex (fun c-> c)
            let ticks=  t.Ticks
            //test <@ (ticks,times.[i].Ticks,i) = (ticks, times.[j].Ticks, j) @> // cix.FindSampleIndex(t) @>
            //test <@ (ticks,i) = (ticks, j) @> // cix.FindSampleIndex(t) @>
            Assert.Equal(j,i)
            t <- t.AddTicks(int64 tinc)



[<Fact>]
let ``jump index 2b``() = 
    let times = [|23 .. 5 .. 32423|] |> Array.map (int64 >> DateTime)
    //let cix = TimeChunkIndex(times)
    let ix = JumpIndex.create 1000 2 times
    let mutable c = JumpCursor(ix)
    let mutable t = DateTime(0L)
    c.index <- 999
    c.Next(times.[1001].AddTicks(2L))
    Assert.Equal(c.index, 1001)

[<Fact>]
let ``jump index 2``() = 
    let times = [|23 .. 5 .. 32423|] |> Array.map (int64 >> DateTime)
    //let cix = TimeChunkIndex(times)
    let ix = JumpIndex.create 1000 2 times
    for tinc in [1;4;5;7;15;200;1000;10000;20000] do 
        let mutable c = JumpCursor(ix)
        let mutable t = DateTime(0L)
        while t.Ticks < 50000L do 
            while t < times.[0] do t <- t.AddTicks(int64 tinc)
            c.index <- 0
            c.Next(t)
            let i = c.index
            let j = binarySearch (>) times 0 (times.Length - 1) t// |> Array.findIndex (fun c-> c)
            let ticks=  t.Ticks
            //test <@ (ticks,times.[i].Ticks,i) = (ticks, times.[j].Ticks, j) @> // cix.FindSampleIndex(t) @>
            //test <@ (ticks,i) = (ticks, j) @> // cix.FindSampleIndex(t) @>
            Assert.Equal(j,i)
            t <- t.AddTicks(int64 tinc)




[<Fact>]
let ``value chunk index``() = 
    let rng = Random(234)
    let values = Array.init 100 (fun _ -> rng.NextDouble()*1000.0)
    let ix = Chunked.indexArray [10] values
    let manual = values |> Array.chunkBySize 10 
    let c = ix.Chunks.[0]
    test <@ c.ChunkCount = 10 @>
    test <@ (manual |> Array.map Array.max) = (Array.init 10 c.Max) @>
    test <@ (manual |> Array.map Array.min) = (Array.init 10 c.Min) @>
    test <@ (manual |> Array.map Array.head) = (Array.init 10 c.First) @>
    test <@ (manual |> Array.map Array.last) = (Array.init 10 c.Last) @>
    test <@ (manual |> Array.map Array.length) = (Array.init 10 c.Count) @>
    test <@ (manual |> Array.map Array.sum) = (Array.init 10 c.Sum) @>
    for v in values do 
        let j = Chunked.jumpArray ix 0 (fun i a -> a.[i] = v ) (fun i a -> a.Max(i) >= v && a.Min(i) <= v)
        let ind = values |> Array.findIndex (fun x -> x = v)
        test <@ ind = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let j = Chunked.jumpArray ix 0 (fun i a -> a.[i] = v ) (fun i a -> a.Max(i) >= v && a.Min(i) <= v)
        let ind = values |> Array.tryFindIndex (fun x -> x = v) |> Option.defaultValue values.Length
        test <@ ind = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let index = rng.Next(100)
        let j = Chunked.jumpArray ix index (fun i a -> a.[i] >= v ) (fun i a -> a.Max(i) >= v)
        let ind = values.[index ..] |> Array.tryFindIndex (fun x -> x >= v) |> Option.defaultValue (values.Length - index)
        test <@ ind + index = j @>

[<Fact>]
let ``value chunk index 2``() = 
    let rng = Random(234)
    let values = Array.init 104 (fun _ -> rng.NextDouble()*1000.0)
    let ix = Chunked.indexArray [10] values
    let manual = values |> Array.chunkBySize 10 
    let c = ix.Chunks.[0]
    let l = 11
    test <@ c.ChunkCount = l @>
    test <@ (manual |> Array.map Array.max) = (Array.init l c.Max) @>
    test <@ (manual |> Array.map Array.min) = (Array.init l c.Min) @>
    test <@ (manual |> Array.map Array.head) = (Array.init l c.First) @>
    test <@ (manual |> Array.map Array.last) = (Array.init l c.Last) @>
    test <@ (manual |> Array.map Array.length) = (Array.init l c.Count) @>
    test <@ (manual |> Array.map Array.sum) = (Array.init l c.Sum) @>
    for v in values do 
        let j = Chunked.jumpArray ix 0 (fun i a -> a.[i] = v ) (fun i a -> a.Max(i) >= v && a.Min(i) <= v)
        let ind = values |> Array.findIndex (fun x -> x = v)
        test <@ ind = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let j = Chunked.jumpArray ix 0 (fun i a -> a.[i] = v ) (fun i a -> a.Max(i) >= v && a.Min(i) <= v)
        let ind = values |> Array.tryFindIndex (fun x -> x = v) |> Option.defaultValue values.Length
        test <@ ind = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let index = rng.Next(100)
        let j = Chunked.jumpArray ix index (fun i a -> a.[i] >= v ) (fun i a -> a.Max(i) >= v)
        let ind = values.[index ..] |> Array.tryFindIndex (fun x -> x >= v) |> Option.defaultValue (values.Length - index)
        test <@ ind + index = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let mutable count = 0
        let j = Chunked.jumpArray ix 0 (fun i a -> count <- count + 1;a.[i] >= v ) (fun i a -> a.Max(i) >= v)
        test <@ count < l*2 @>
        let ind = values |> Array.tryFindIndex (fun x -> x >= v) |> Option.defaultValue (values.Length)
        test <@ ind = j @>

[<Fact>]
let ``value chunk index 3``() = 
    let rng = Random(234)
    let values = Array.init 1000 (fun _ -> rng.NextDouble()*1000.0)
    let ix = Chunked.indexArray [10;10] values
    for v in values do 
        let j = Chunked.jumpArray ix 0 (fun i a -> a.[i] = v ) (fun i a -> a.Max(i) >= v && a.Min(i) <= v)
        let ind = values |> Array.findIndex (fun x -> x = v)
        test <@ ind = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let j = Chunked.jumpArray ix 0 (fun i a -> a.[i] = v ) (fun i a -> a.Max(i) >= v && a.Min(i) <= v)
        let ind = values |> Array.tryFindIndex (fun x -> x = v) |> Option.defaultValue values.Length
        test <@ ind = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let index = rng.Next(100)
        let j = Chunked.jumpArray ix index (fun i a -> a.[i] >= v ) (fun i a -> a.Max(i) >= v)
        let ind = values.[index ..] |> Array.tryFindIndex (fun x -> x >= v) |> Option.defaultValue (values.Length - index)
        test <@ ind + index = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let mutable count = 0
        let j = Chunked.jumpArray ix 0 (fun i a -> count <- count + 1;a.[i] >= v ) (fun i a -> a.Max(i) >= v)
        test <@ count < 30 @>
        let ind = values |> Array.tryFindIndex (fun x -> x >= v) |> Option.defaultValue (values.Length)
        test <@ ind = j @>

[<Fact>]
let ``value chunk index 4``() = 
    let rng = Random(234)
    let values = Array.init 1432 (fun _ -> rng.NextDouble()*1000.0)
    let ix = Chunked.indexArray [10;10] values
    for v in values do 
        let j = Chunked.jumpArray ix 0 (fun i a -> a.[i] = v ) (fun i a -> a.Max(i) >= v && a.Min(i) <= v)
        let ind = values |> Array.findIndex (fun x -> x = v)
        test <@ ind = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let j = Chunked.jumpArray ix 0 (fun i a -> a.[i] = v ) (fun i a -> a.Max(i) >= v && a.Min(i) <= v)
        let ind = values |> Array.tryFindIndex (fun x -> x = v) |> Option.defaultValue values.Length
        test <@ ind = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let index = rng.Next(100)
        let j = Chunked.jumpArray ix index (fun i a -> a.[i] >= v ) (fun i a -> a.Max(i) >= v)
        let ind = values.[index ..] |> Array.tryFindIndex (fun x -> x >= v) |> Option.defaultValue (values.Length - index)
        test <@ ind + index = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let mutable count = 0
        let j = Chunked.jumpArray ix 0 (fun i a -> count <- count + 1;a.[i] >= v ) (fun i a -> a.Max(i) >= v)
        test <@ count < 30 @>
        let ind = values |> Array.tryFindIndex (fun x -> x >= v) |> Option.defaultValue (values.Length)
        test <@ ind = j @>


[<Fact>]
let ``value chunk index 5``() = 
    let rng = Random(234)
    let values = Array.init 10000 (fun _ -> rng.NextDouble()*1000.0)
    let ix = Chunked.indexArray [2;2;2;2;2;2;2;2] values
    for v in values do 
        let j = Chunked.jumpArray ix 0 (fun i a -> a.[i] = v ) (fun i a -> a.Max(i) >= v && a.Min(i) <= v)
        let ind = values |> Array.findIndex (fun x -> x = v)
        test <@ ind = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let j = Chunked.jumpArray ix 0 (fun i a -> a.[i] = v ) (fun i a -> a.Max(i) >= v && a.Min(i) <= v)
        let ind = values |> Array.tryFindIndex (fun x -> x = v) |> Option.defaultValue values.Length
        test <@ ind = j @>
    for i = 0 to 1000 do 
        let v = rng.NextDouble()*1000.0
        let index = rng.Next(100)
        let j = Chunked.jumpArray ix index (fun i a -> a.[i] >= v ) (fun i a -> a.Max(i) >= v)
        let ind = values.[index ..] |> Array.tryFindIndex (fun x -> x >= v) |> Option.defaultValue (values.Length - index)
        test <@ ind + index = j @>
    for i = 0 to 10000 do 
        let v = rng.NextDouble()*1000.0
        let mutable count = 0
        let j = Chunked.jumpArray ix 0 (fun i a -> count <- count + 1;a.[i] >= v ) (fun i a -> a.Max(i) >= v)
        test <@ count < 120 @>
        let ind = values |> Array.tryFindIndex (fun x -> x >= v) |> Option.defaultValue (values.Length)
        test <@ ind = j @>



[<Fact>]
let ``value chunk iter 1``() = 
    let values = Array.init 10 double
    let ix = Chunked.indexArray [2] values
    let si = 2
    let ei = 4
    let sumValues = ResizeArray()
    let sum = values.[si .. ei] |> Array.sum
    let mutable s = 0.0
    Chunked.iterArraySlice ix si ei 
        (fun v -> 
            sumValues.Add(-1,v)
            s <- v + s)
        (fun x c -> 
            sumValues.Add(c,x.Sum(c))
            s <- s + x.Sum(c))
    test <@ sum = s @>


[<Fact>]
let ``value chunk iter 2``() = 
    let rng = Random(234)
    let values = Array.init 100 (fun _ -> rng.NextDouble()*1000.0)
    let ix = Chunked.indexArray [10] values
    let si = 23
    let ei = 68
    let sum = values.[si .. ei] |> Array.sum
    let mutable s = 0.0
    Chunked.iterArraySlice ix si ei 
        (fun v -> s <- v + s)
        (fun x c -> s <- s + x.Sum(c))
    test <@ abs(sum - s) < 10e-10 @>


[<Fact>]
let ``value chunk iter 3``() = 
    let rng = Random(234)
    let values = Array.init 100 (fun _ -> rng.NextDouble()*1000.0)
    let ix = Chunked.indexArray [10] values
    for i = 0 to 1000 do 
        let a = rng.Next(100)
        let b = rng.Next(100)
        let si = min a b
        let ei = max a b
        let sum = values.[si .. ei] |> Array.sum
        let mutable count = 0
        let mutable s = 0.0
        Chunked.iterArraySlice ix si ei 
            (fun v -> 
                count <- count + 1
                s <- v + s)
            (fun x c -> s <- s + x.Sum(c))
        test <@ abs(sum - s) < 10e-10 @>
        test <@ count <= 22 @>

        
[<Fact>]
let ``value chunk iter 4``() = 
    let rng = Random(234)
    let values = Array.init 1000 (fun _ -> rng.NextDouble()*1000.0)
    let ix = Chunked.indexArray [10;10] values
    for i = 0 to 1000 do 
        let a = rng.Next(1000)
        let b = rng.Next(1000)
        let si = min a b
        let ei = max a b
        let sum = values.[si .. ei] |> Array.sum
        let mutable count = 0
        let mutable s = 0.0
        Chunked.iterArraySlice ix si ei 
            (fun v -> 
                count <- count + 1
                s <- v + s)
            (fun x c -> s <- s + x.Sum(c))
        test <@ abs(sum - s) < 10e-10 @>
        test <@ count <= 22 @>


[<Fact>]
let ``value chunk iter 5``() = 
    let rng = Random(234)
    let values = Array.init 1000 (fun _ -> rng.NextDouble()*1000.0)
    let ix = Chunked.indexArray [3;3;3;5] values
    for i = 0 to 1000 do 
        let a = rng.Next(1000)
        let b = rng.Next(1000)
        let si = min a b
        let ei = max a b
        let sum = values.[si .. ei] |> Array.sum
        let mutable count = 0
        let mutable s = 0.0
        Chunked.iterArraySlice ix si ei 
            (fun v -> 
                count <- count + 1
                s <- v + s)
            (fun x c -> s <- s + x.Sum(c))
        test <@ abs(sum - s) < 10e-10 @>
        test <@ count <= 10 @>



[<Fact>]
let ``value chunk iter vector 1``() = 
    let rng = Random(234)
    let values = Array.init 10 double
    let ix = Chunked.indexArray [2] values
    let si = 0
    let ei = 2
    let sum = values.[si .. ei] |> Array.sum
    let mutable s = 0.0
    let sz = Vector<double>.Count
    Chunked.iterArraySliceVector ix si ei 
        (fun a si ei -> 
            let mutable i = si
            let mutable ss = Vector.Zero
            while i <= ei - sz do 
                let av = Vector(a,i)
                ss <- Vector.Add(ss,av)
                i <- i + sz
            s <- s + Vector.Dot(ss,Vector.One)
            while i <= ei do 
                s <- s + a.[i]
                i <- i + 1
        )
        (fun a si ei -> 
            let a = (a :?> double ArrayChunked).SumArray
            let mutable i = si
            let mutable ss = Vector.Zero
            while i <= ei - sz do 
                let av = Vector(a,i)
                ss <- Vector.Add(ss,av)
                i <- i + sz
            s <- s + Vector.Dot(ss,Vector.One)
            while i <= ei do 
                s <- s + a.[i]
                i <- i + 1
        )
    test <@ abs(sum - s) < 10e-10 @>


[<Fact>]
let ``value chunk iter vector 2``() = 
    let rng = Random(234)
    let values = Array.init 10 double
    let ix = Chunked.indexArray [2] values
    let si = 1
    let ei = 5
    let sum = values.[si .. ei] |> Array.sum
    let mutable s = 0.0
    let sz = Vector<double>.Count
    Chunked.iterArraySliceVector ix si ei 
        (fun a si ei -> 
            let mutable i = si
            let mutable ss = Vector.Zero
            while i <= ei - sz do 
                let av = Vector(a,i)
                ss <- Vector.Add(ss,av)
                i <- i + sz
            s <- s + Vector.Dot(ss,Vector.One)
            while i <= ei do 
                s <- s + a.[i]
                i <- i + 1
        )
        (fun a si ei -> 
            let a = (a :?> double ArrayChunked).SumArray
            let mutable i = si
            let mutable ss = Vector.Zero
            while i <= ei - sz do 
                let av = Vector(a,i)
                ss <- Vector.Add(ss,av)
                i <- i + sz
            s <- s + Vector.Dot(ss,Vector.One)
            while i <= ei do 
                s <- s + a.[i]
                i <- i + 1
        )
    test <@ abs(sum - s) < 10e-10 @>

[<Fact>]
let ``value chunk iter vector 3``() = 
    let rng = Random(234)
    let values = Array.init 10000 (fun _ -> rng.NextDouble()*1000.0)
    let ix = Chunked.indexArray [100] values
    let si = 1232
    let ei = 6043
    let sum = values.[si .. ei] |> Array.sum
    let mutable s = 0.0
    let sz = Vector<double>.Count
    Chunked.iterArraySliceVector ix si ei 
        (fun a si ei -> 
            let mutable i = si
            let mutable ss = Vector.Zero
            while i <= ei - sz do 
                let av = Vector(a,i)
                ss <- Vector.Add(ss,av)
                i <- i + sz
            s <- s + Vector.Dot(ss,Vector.One)
            while i <= ei do 
                s <- s + a.[i]
                i <- i + 1
        )
        (fun a si ei -> 
            let a = (a :?> double ArrayChunked).SumArray
            let mutable i = si
            let mutable ss = Vector.Zero
            while i <= ei - sz do 
                let av = Vector(a,i)
                ss <- Vector.Add(ss,av)
                i <- i + sz
            s <- s + Vector.Dot(ss,Vector.One)
            while i <= ei do 
                s <- s + a.[i]
                i <- i + 1
        )
    test <@ abs(sum - s) < 1e-8 @>



[<Fact>]
let ``value chunk iter vector 4``() = 
    let rng = Random(234)
    let values = Array.init 30000 (fun _ -> rng.NextDouble()*1000.0)
    let ix = Chunked.indexArray [16;16;16] values
    for i = 0 to 1000 do 
        let i1 = rng.Next(30000)
        let i2 = rng.Next(30000)
        let si = min i1 i2
        let ei = max i1 i2
        let sum = values.[si .. ei] |> Array.sum
        let mutable count = 0
        let mutable s = 0.0
        let sz = Vector<double>.Count
        Chunked.iterArraySliceVector ix si ei 
            (fun a si ei -> 
                let mutable i = si
                let mutable ss = Vector.Zero
                while i <= ei - sz do 
                    count <- count + 1
                    let av = Vector(a,i)
                    ss <- Vector.Add(ss,av)
                    i <- i + sz
                s <- s + Vector.Dot(ss,Vector.One)
                while i <= ei do 
                    count <- count + 1
                    s <- s + a.[i]
                    i <- i + 1
            )
            (fun a si ei -> 
                let a = (a :?> double ArrayChunked).SumArray
                let mutable i = si
                let mutable ss = Vector.Zero
                while i <= ei - sz do 
                    count <- count + 1
                    let av = Vector(a,i)
                    ss <- Vector.Add(ss,av)
                    i <- i + sz
                s <- s + Vector.Dot(ss,Vector.One)
                while i <= ei do 
                    count <- count + 1
                    s <- s + a.[i]
                    i <- i + 1
            )
        test <@ abs(sum - s) < 1e-5 @>
        test <@ count < 2000 @>


open Trebuchet.Collections
[<Fact>]
let ``MemResizeArray expand size``() =  
    let name = IO.Path.GetTempFileName()
    try
        use ra = MemResizeArray.Create(name)
        for i = 0 to 10000  do
            ra.Add(byte i)
        let dat = [|0 .. 10000|] |> Array.map byte
        test <@ dat = ra.ToArray() @>
        ra.Dispose()
    finally 
        IO.File.Delete(name)
[<Fact>]
let ``MemResizeArray simple tests 1``() =  
    let name = IO.Path.GetTempFileName()
    try
        use ra = MemResizeArray.Create(name)
        for i = 0 to 10000  do
            ra.Add(int64 i)
        let dat = [|0L .. 10000L|]
        test <@ ra.ToArray() = dat @>
        use ma = ra.ToMemArray()
        test <@ ma.[2000] = 2000L @>
        test <@ ma.[10000] = 10000L @>
        ma.Dispose()
        ra.Dispose()
        use ma2 = MemArray<int64>.ReadWrite(name)
        test <@ ma2.Length = 10001 @>
        test <@ ma2.[10000] = 10000L @>
        test <@ ma2.[3243] = 3243L @>
        test <@ ma2.[0] = 0L @>
        ma2.Dispose()
        use ra2 = MemResizeArray.Append(name)
        for i = 0 to 20000  do
            ra2.Add(int64 i)
        use ma3 = ra2.ToMemArray()
        test <@ ma3.[10000] = 10000L @>
        test <@ ma3.[10001] = 0L @>
        test <@ ma3.[20001] = 10000L @>
        ma3.Dispose()
        ra2.Dispose()
        use ma4 = MemArray<int64>.ReadWrite(name)
        test <@ ma4.Length = 30002 @>
        test <@ ma4.[ma4.Length - 1] = 20000L @>
        test <@ ma4.[ma4.Length - 2] = 19999L @>
    finally 
        IO.File.Delete(name)
