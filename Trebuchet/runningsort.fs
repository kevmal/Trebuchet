namespace Trebuchet.DataStructures




open System
open System.IO
open System.Collections.Concurrent
open System.Collections.Generic
type RunningSort<'a when 'a : comparison>(length : int) = 
    let values = Array.zeroCreate length
    let q = Queue()
    member x.Add(v : 'a) = 
        let removeIndex = 
            if q.Count = length then 
                let toRemove = q.Dequeue()
                Array.BinarySearch(values, toRemove)
            else
                q.Count
        q.Enqueue v
        assert(removeIndex >= 0)
        let insertIndex = 
            let i = Array.BinarySearch(values, 0, q.Count, v)
            if i < 0 then 
                ~~~i
            else
                i
        if insertIndex > removeIndex then 
            for i = removeIndex to insertIndex - 2 do 
                values.[i] <- values.[i + 1]
            values.[insertIndex - 1] <- v
        elif insertIndex < removeIndex then 
            for i = removeIndex downto insertIndex + 1 do 
                values.[i] <- values.[i - 1]
            values.[insertIndex] <- v
        else    
            values.[insertIndex] <- v
        assert(Array.truncate q.Count values = (values |> Array.truncate q.Count |> Array.sort))
    member x.Max = values.[q.Count - 1]
    member x.Min = values.[0]
    member x.Quantile(quant : double) = 
        let i = (quant*double q.Count) |> round |> max 0.0 |> int |> min (q.Count - 1)
        values.[i]
    member x.QuantileRank(value : 'a) = 
        let i = Array.BinarySearch(values,value)
        if i < 0 then 
            let i = ~~~i
            if i = 0 then 
                0.0
            else 
                (double i - 0.5) / double q.Count
        else 
            let mutable count = 0
            let mutable i = i
            let mutable j = i 
            while j >= 0 && values.[j] = value do 
                count <- count + 1 
                j <- j - 1
                i <- i - 1
            j <- i + 1
            while j < values.Length && values.[j] = value do 
                count <- count + 1
                j <- j + 1
            (double i + 0.5*double count) / double q.Count
    member x.Values = values
    member x.Queue = q
    member x.Reset() = 
        q.Clear()
        
        
            
