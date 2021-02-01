// Learn more about F# at http://fsharp.org

open System
open BenchmarkDotNet.Running
open Trebuchet
open Trebuchet.Util
open BenchmarkDotNet.Attributes
open Trebuchet.DataIndex
open System.IO


type QuickStart() = 
    let times = //Array.init 10000000 (fun x -> DateTime(1900,1,1).AddMinutes(double x))
        let fname = @"D:\Data\pticks\ESH19\time"
        let fi = FileInfo(fname)
        use b = new BinaryReader(System.IO.File.OpenRead(fname))
        let l = fi.Length / 8L |> int
        let a =  Array.init l (fun _ -> b.ReadInt64() |> DateTime)
        a

    let moreTimes = //Array.init 1000 (fun x -> times.[0].AddHours(double x))
        let r = Random(3425234)
        times |> Array.map (fun x -> x.AddMilliseconds(r.NextDouble()*10.0 - 5.0))
    let tc = TimeChunkIndex(times)
    let tcm = TimeChunkIndexMem(times)
    let tc2 = TimeChunkIndex(times,times.Length)
    let tc4 = TimeChunkIndex(times,times.Length).FindSampleIndex
    let ji = JumpIndex.create 32 32 times
        
    [<Benchmark>]
    member x.BaseLine() = 
        for i = 0 to moreTimes.Length - 1 do
            let j = moreTimes.[i].AddDays(2.0)
            () 
    [<Benchmark>]
    member x.BinarySearch() = 
        for i = 0 to moreTimes.Length - 1 do
            let j = binarySearch (fun (x : DateTime) (y : DateTime) -> x.Ticks > y.Ticks) times 0 (times.Length - 1) moreTimes.[i]
            ()
    [<Benchmark>]
    member x.BinarySearchRegularCompare() = 
        for i = 0 to moreTimes.Length - 1 do
            let j = binarySearch (>) times 0 (times.Length - 1) moreTimes.[i]
            ()
    [<Benchmark>]
    member x.TimeChunk() = 
        for i = 0 to moreTimes.Length - 1 do
            let j = tc.FindSampleIndex(moreTimes.[i])
            ()
    [<Benchmark>]
    member x.TimeChunk2() = 
        for i = 0 to moreTimes.Length - 1 do
            let j = tcm.FindSampleIndex(moreTimes.[i])
            ()
    [<Benchmark>]
    member x.TimeChunkInterface2() = 
        for i = 0 to moreTimes.Length - 1 do
            let j = tc4 moreTimes.[i]
            ()
    [<Benchmark>]
    member x.TimeChunkSmaller() = 
        for i = 0 to moreTimes.Length - 1 do
            let j = tc2.FindSampleIndex(moreTimes.[i])
            ()
    [<Benchmark>]
    member x.JustAccess() = 
        for i = 0 to moreTimes.Length - 1 do
            let j = moreTimes.[i].AddTicks(times.[i].Ticks)
            ()
    [<Benchmark>]
    member x.JumpIndex() = 
        let jc = JumpCursor(ji)
        for i = 0 to moreTimes.Length - 1 do    
            jc.Next(moreTimes.[i])
            ()



type ArrayMath() = 
    let rng = Random(3432)
    let a = Array.init 1000000 (fun _ -> rng.Next() |> float32)
    let b = Array.init 1000000 (fun _ -> rng.Next() |> float32)
    let c = Array.zeroCreate 1000000
        
    let a2 = MathNet.Numerics.LinearAlgebra.CreateVector.DenseOfArray(a)
    let b2 = MathNet.Numerics.LinearAlgebra.CreateVector.DenseOfArray(b)
    let c2 = MathNet.Numerics.LinearAlgebra.CreateVector.DenseOfArray(c)
    
    [<Benchmark>]
    member x.Naive() = 
        for i = 0 to c.Length - 1 do
            c.[i] <- a.[i] + b.[i]
        c
    [<Benchmark>]
    member x.SimdOnArray() =
        V.add c a b
    [<Benchmark>]
    member x.SimdOnArray2() = V a + V b
    [<Benchmark>]
    member x.ILGPUCpu() = ILGPU.Say.add c a b
    [<Benchmark>]
    member x.NaiveFSharp() = (a,b) ||> Array.iteri2 (fun i a b -> c.[i] <- a + b)
    [<Benchmark>]
    member x.MathNet() = a2.Add(b2,c2)
    //[<Benchmark>]
    //member x.MathNet2() = MathNet.Numerics.LinearAlgebra.CreateVector.DenseOfArray(a).Add(MathNet.Numerics.LinearAlgebra.CreateVector.DenseOfArray(b),c2)


            
                    
        
        
[<EntryPoint>]
let main argv =
    let summary = BenchmarkRunner.Run<QuickStart>()
    printfn "Hello World from F#!"
    0 // return an integer exit code
