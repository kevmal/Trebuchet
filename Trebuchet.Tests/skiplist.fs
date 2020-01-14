﻿module skiplist

open Xunit
open Trebuchet.DataStructures
open System



[<Fact>]
let simple1() = 
    let e = SkipList.Entry.Create 299
    let r = Random(232)
    {0.. 10000}
    |> Seq.iter (fun v -> SkipList.addWithPromote r 0.5 10 v e |> ignore)
    Assert.Equal(1, SkipList.valueCount 23 e)
    Assert.Equal(2, SkipList.valueCount 299 e)
    Assert.Equal(10002, SkipList.totalCount e)
    let c = SkipList.remove 299 e
    Assert.Equal(1, c)
    Assert.Equal(10001, SkipList.totalCount e)
    Assert.Equal(1, SkipList.valueCount 299 e)
    Assert.Equal(0, SkipList.remove 300 e)
    Assert.Equal(0, SkipList.valueCount 300 e)
    Assert.Equal(10000, SkipList.totalCount e)

[<Fact>]
let ``should reverse``() = 
    let e = SkipList.Entry.Create 299
    let r = Random(232)
    {0.. 10000}
    |> Seq.rev
    |> Seq.iter (fun v -> SkipList.addWithPromote r 0.5 10 v e |> ignore)
    let expected = 299 :: [0 .. 10000] |> Seq.sort |> Seq.toArray
    let a = e |> SkipList.items |> Seq.toArray
    Assert.Equal(expected.Length, a.Length)
    (expected,a)
    ||> Array.iter2 (fun e a -> Assert.Equal(e,a))
    


[<Fact>]
let ``sorting random``() = 
    let e = SkipList.Entry.Create 322.0
    let r = Random(232)
    let r2 = Random(2349)
    let ra = ResizeArray()
    ra.Add 322.0
    for i = 1 to 5000 do 
        let v = r2.NextDouble()
        ra.Add v
        SkipList.addWithPromote r 0.5 10 v e  |> ignore
    let sorted = ra.ToArray() |> Array.sort
    let a = e |> SkipList.items |> Seq.toArray
    (sorted,a)
    ||> Array.iter2 (fun e a -> Assert.Equal(e,a))
    


[<Fact>]
let ``sorting random with removes``() = 
    let e = SkipList.singleton 322.0
    let r = Random(232)
    let r2 = Random(2349)
    let ra = ResizeArray()
    ra.Add 322.0
    for i = 1 to 5000 do 
        let v = r2.NextDouble()
        ra.Add v
        SkipList.addWithPromote r 0.5 10 v e  |> ignore
        if r2.NextDouble() < 0.3 then 
            let i = r2.Next(ra.Count)
            let r = ra.[i]
            ra.RemoveAt i
            SkipList.remove r e |> ignore
    let sorted = ra.ToArray() |> Array.sort
    let a = e |> SkipList.items |> Seq.toArray
    (sorted,a)
    ||> Array.iter2 (fun e a -> Assert.Equal(e,a))


