namespace Trebuchet.DataStructures

open System.Runtime.CompilerServices
open System.Collections.Generic

[<AutoOpen>]
module internal CirculatBufferHelp = 
    let inline imod a b = 
       let r = a % b
       if r < 0 then r + b else r


type CircularBuffer<'a>(a : 'a []) = 
    let mutable count = 0
    let mutable index = 0
    let length = a.Length
    member x.Length = length
    member x.Count = count
    new(length) = CircularBuffer(Array.zeroCreate length)
    member x.BaseArray = a
    member x.Add(item) = 
        a.[index] <- item
        index <- (index + 1) % length
        count <- min (count + 1) length
    member x.Insert(insertIndex : int, value : 'a, removeIndex : int) = 
        if insertIndex = removeIndex then 
            let last = x.[insertIndex]
            x.[insertIndex] <- value
            last
        elif removeIndex > insertIndex then 
            let mutable i = insertIndex
            let mutable v = value 
            while i <= removeIndex do 
                let last = x.[i]
                x.[i] <- v
                v <- last
                i <- i + 1
            v
        else
            let mutable i = insertIndex
            let mutable v = value 
            while i >= removeIndex do 
                let last = x.[i]
                x.[i] <- v
                v <- last
                i <- i - 1
            v
    member x.Item 
        with get(i : int) = a.[imod (index - count + i) length]
        and set i v = a.[imod (index - count + i) length] <- v
    member x.Clear(): unit = 
        count <- 0 
        index <- 0
    member this.Contains(item: 'a): bool = 
        match item :> obj with 
        | null -> 
            let mutable found = false   
            let mutable i = 0
            while not found && i < count do 
                found <- isNull (this.[i] :> obj)
            found
        | _ -> 
            let mutable found = false   
            let c = EqualityComparer<'a>.Default
            let mutable i = 0
            while not found && i < count do 
                found <- c.Equals(this.[i], item)
            found
    interface IList<'a> with
        member this.Add(item: 'a): unit = this.Add item
        member this.Clear(): unit = this.Clear()
        member this.Contains(item: 'a): bool = this.Contains(item)
        member this.CopyTo(array: 'a [], arrayIndex: int): unit = 
            for i = 0 to count - 1 do 
                array.[i] <- this.[i]
        member this.Count: int = count
        member this.GetEnumerator(): IEnumerator<'a> = 
            (seq {
                for i = 0 to count - 1 do 
                    this.[i]
            }).GetEnumerator()
        member this.GetEnumerator(): System.Collections.IEnumerator = 
            (seq {
                for i = 0 to count - 1 do 
                    this.[i]
            }).GetEnumerator() :> _
        member this.IndexOf(item: 'a): int = 
            let mutable found = false   
            let c = EqualityComparer<'a>.Default
            let mutable i = 0
            while not found && i < count do 
                found <- c.Equals(this.[i], item)
            if found then i else -1
        member this.Insert(index: int, item: 'a): unit = 
            this.Insert(index, item, 0) |> ignore
        member this.IsReadOnly: bool = false
        member this.Item
            with get (index: int): 'a = 
                this.[index]
            and set (index: int) (v: 'a): unit = 
                this.[index] <- v
        member this.Remove(item: 'a): bool = 
            raise (System.NotImplementedException())
        member this.RemoveAt(index: int): unit = 
            raise (System.NotImplementedException())    


module CircularBuffer = 
    let rec binarySearch (a : 'a CircularBuffer) (si : int) (ei : int) v = 
       if v < a.[si] then
           -1
       elif si > ei || v > a.[ei] then
           ~~~ei - 1
       elif si = ei then   
           if a.[si] = v then
               si
           else
               ~~~si
       else
           let m = (ei - si) / 2 + si
           if a.[m] = v then
               m
           elif a.[m] > v then
               binarySearch a si (m - 1) v
           else
               binarySearch a (m + 1) ei v

open CircularBuffer
[<Extension>]
type CircularBufferEx() = 
   [<Extension>]
   static member BinarySearch(a, value) = binarySearch a 0 (a.Count - 1) value
   //[<Extension>]
   //static member AddSorted(a, value) = 
   //    let insertIndex = binarySearch a 0 (a.Count - 1) value
   //    a.Insert(insertIndex, value, 0)
   
