module runningsort

open Xunit
open Trebuchet.DataStructures
open System
open Trebuchet.DataStructures
open System.Collections.Generic



[<Fact>]
let ``int against resizearray impl``() = 
    let x = RunningSort(100)
    let r = Random(324)
    let a = ResizeArray()
    let q = Queue()

    for i = 0 to 10000 do 
        let n = r.Next(20)
        if q.Count = 100 then 
            a.Remove(q.Dequeue()) |> ignore
        q.Enqueue(n)
        a.Add(n)
        a.Sort()
        x.Add(n)
        Assert.True((x.Values |> Array.truncate q.Count) = a.ToArray())
        Assert.Equal(a |> Seq.max, x.Max)
        Assert.Equal(a |> Seq.min, x.Min)
    


[<Fact>]
let ``double against resizearray impl``() = 
    let x = RunningSort(100)
    let r = Random(324)
    let a = ResizeArray()
    let q = Queue()

    for i = 0 to 10000 do 
        let n = r.NextDouble()
        if q.Count = 100 then 
            a.Remove(q.Dequeue()) |> ignore
        q.Enqueue(n)
        a.Add(n)
        a.Sort()
        x.Add(n)
        Assert.True((x.Values |> Array.truncate q.Count) = a.ToArray())
        Assert.Equal(a |> Seq.max, x.Max)
        Assert.Equal(a |> Seq.min, x.Min)
    











