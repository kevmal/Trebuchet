module skiplist

open Xunit
open Trebuchet.DataStructures
open System
open Trebuchet.DataStructures



//[<Fact(Skip = "")>]
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
let ``sorting random with removes int``() = 
    let e = SkipList.singleton 322
    let r = Random(232)
    let r2 = Random(2349)
    let ra = ResizeArray()
    ra.Add 322
    for i = 1 to 5000 do 
        let v = r2.Next(3000)
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
    // remove all but one
    while ra.Count > 1 do 
        let i = r2.Next(ra.Count)
        let r = ra.[i]
        ra.RemoveAt i
        SkipList.remove r e |> ignore
    Assert.Equal(ra.[0], e.Value)
        

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
    // remove all but one
    while ra.Count > 1 do 
        let i = r2.Next(ra.Count)
        let r = ra.[i]
        ra.RemoveAt i
        SkipList.remove r e |> ignore
    Assert.Equal(ra.[0], e.Value)
        


[<Fact>]
let ``int indexing 1``() = 
    let e = SkipList.singleton 0
    let ra = ResizeArray([1 .. 1000])
    let r = Random(232)
    let r2 = Random(2349)
    while ra.Count > 0 do 
        let i = r2.Next(ra.Count)
        let v = ra.[i]
        ra.RemoveAt i
        SkipList.addWithPromote r 0.5 10 v e  |> ignore
    SkipList.remove 0 e |> ignore
    Assert.Equal(1000, SkipList.totalCount e)
    let last = SkipList.nth 999 e
    Assert.Equal(1000, last.Value)
    [0 .. 999]
    |> List.iter 
        (fun i ->
            let n = SkipList.nth i e
            Assert.Equal(i + 1, n.Value)
        )

























