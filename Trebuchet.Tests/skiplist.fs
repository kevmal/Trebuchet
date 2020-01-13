module skiplist

open Xunit
open Trebuchet.DataStructures



[<Fact>]
let simple1() = 
    let l = SkipList()
    {0.. 10000}
    |> Seq.iter l.Add
    Assert.Equal(10001, l.Count)
    let readBack = l |> Seq.toArray
    Assert.Equal(0, readBack.[0])
    Assert.Equal(2932, readBack.[2932])
    Assert.Equal(5653, readBack.[5653])
    Assert.Equal(5654, readBack.[5654])
    Assert.Equal(9999, readBack.[9999])
    Assert.Equal(10000, readBack.[10000])

[<Fact>]
let simple2() = 
    let l = SkipList()
    [4;4;4;4;4] |> Seq.iter l.Add
    Assert.Equal(1, l.Count)
    Assert.Equal(l.[0], 4)
    Assert.Equal(l.Max, 4)
    Assert.Equal(l.Min, 4)


[<Fact>]
let simple3() = 
    let l = SkipList()
    {0 .. 10} |> Seq.rev |> Seq.iter l.Add
    Assert.Equal(11, l.Count)
    Assert.Equal(l.[0], 0)
    Assert.Equal(l.[9], 9)
    Assert.Equal(l.[10], 10)


