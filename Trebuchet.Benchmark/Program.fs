// Learn more about F# at http://fsharp.org

open System
open BenchmarkDotNet.Running
open Trebuchet
open Trebuchet.Util
open BenchmarkDotNet.Attributes
open Trebuchet.DataIndex
open System.IO
open System.Runtime.CompilerServices


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

let f s =  (s <<< 1) + 1L
let inline f2 s =  (s <<< 1) + 1L

[<Struct>]
type Crap(i : int) = 
    member x.F(s) =  (s <<< 1) + 1L
[<Struct>]
type Crap2(i : int) = 
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member x.F(s) =  (s <<< 1) + 1L
type ICrap = 
    abstract member F : int64 -> int64
[<Struct>]
type Crap3(i : int) = 
    member x.F(s) =  (s <<< 1) + 1L
    interface ICrap with
        member x.F(s) = x.F(s)

type Crap4<'a when 'a : struct and 'a :> ICrap>(a : 'a) = 
    member x.F(s) =  a.F s

type Crap5(i : int) = 
    member x.F(s) =  (s <<< 1) + 1L
    interface ICrap with
        member x.F(s) = x.F(s)

type Crap6<'a when 'a :> ICrap>(a : 'a) = 
    member x.F(s) =  a.F s

type Poo() = 
    [<Benchmark>]
    member x.Best() = 
        let mutable s = 1L
        for i = 0 to 1000000 do
            s <- (s <<< 1) + 1L
        s
    [<Benchmark>]
    member x.FsCall() =
        let mutable s = 1L
        for i = 0 to 1000000 do
            s <- f s
        s
    [<Benchmark>]
    member x.FsInlineCall() =
        let mutable s = 1L
        for i = 0 to 1000000 do
            s <- f2 s
        s
    [<Benchmark>]
    member x.StructCall() =
        let c = Crap 1
        let mutable s = 1L
        for i = 0 to 1000000 do
            s <- c.F s
        s
    [<Benchmark>]
    member x.StructCallInlining() =
        let c = Crap2 1
        let mutable s = 1L
        for i = 0 to 1000000 do
            s <- c.F s
        s
    [<Benchmark>]
    member x.InterfaceObjCtor() =
        let c = {new ICrap with member x.F(s) = (s <<< 1) + 1L}
        let mutable s = 1L
        for i = 0 to 1000000 do
            s <- c.F s
        s
    [<Benchmark>]
    member x.Interface() =
        let c = Crap3(1) :> ICrap
        let mutable s = 1L
        for i = 0 to 1000000 do
            s <- c.F s
        s
    [<Benchmark>]
    member x.InterfaceGeneric() =
        let c = Crap4(Crap3(1))
        let mutable s = 1L
        for i = 0 to 1000000 do
            s <- c.F s
        s
    [<Benchmark>]
    member x.InterfaceGenericNoStruct() =
        let c = Crap6(Crap5(1))
        let mutable s = 1L
        for i = 0 to 1000000 do
            s <- c.F s
        s
    //[<Benchmark>]
    //member x.MathNet2() = MathNet.Numerics.LinearAlgebra.CreateVector.DenseOfArray(a).Add(MathNet.Numerics.LinearAlgebra.CreateVector.DenseOfArray(b),c2)


            
                                
                    
        
        
[<EntryPoint>]
let main argv =
    let summary = BenchmarkRunner.Run<Poo>()
    printfn "Hello World from F#!"
    0 // return an integer exit code
