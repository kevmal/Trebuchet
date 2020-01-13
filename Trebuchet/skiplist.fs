namespace Trebuchet.DataStructures




open System
open System.IO
open System.Collections.Concurrent
open System.Collections.Generic



module SkipList =



    [<Struct>]
    type AddResult<'a when 'a : comparison> = 
        | Added
        | Node of Entry<'a>
        | NoOp

    and [<AbstractClass; AllowNullLiteral>] Entry<'a when 'a : comparison>() = 
        member val Left : Entry<'a> = null with get,set
        member val Right : Entry<'a> = null with get,set
        member val Down : Entry<'a> = null with get,set
        member val Up : Entry<'a> = null with get,set
        member val Width = 1 with get,set
        abstract member UpdateWidth : unit -> unit
        abstract member Value : 'a
        abstract member Max : 'a
        abstract member Min : 'a
        abstract member Count : int
        abstract member Add : (unit -> bool)*'a -> AddResult<'a>
        abstract member Remove : 'a -> bool
        abstract member Contains : 'a -> bool

    type ValueEntry<'a when 'a : comparison>() = 
        inherit Entry<'a>()
        static let pool = ConcurrentQueue()
        static let del (o : ValueEntry<'a>) = 
            if pool.Count < ValueEntry<'a>.PoolSize then 
                o.Reset()
                pool.Enqueue o
        let mutable count = 1
        member x.Reset() = 
            count <- 1
            x.Width <- 1
            x.Left <- null
            x.Right <- null
            x.Down <- null
            x.Up <- null
        static member Create<'a>() : ValueEntry<'a> = 
            let scc,v = pool.TryDequeue()
            if scc then v else ValueEntry()
        static member val PoolSize = 16
        member val Valuev = Unchecked.defaultof<_> with get,set
        override x.UpdateWidth() = ()
        override x.Value = x.Valuev
        override x.Max = x.Valuev
        override x.Min = x.Valuev
        override x.Count = count
        override x.Add(r, value : 'a) : AddResult<'a> =
            if value > x.Value then 
                match x.Right with 
                | null -> 
                    let ve = ValueEntry.Create()
                    ve.Valuev <- value 
                    ve.Left <- x
                    x.Right <- ve
                    Node (ve :> Entry<'a>)
                | q -> 
                    if q.Value > value then 
                        let ve = ValueEntry.Create()
                        ve.Valuev <- value
                        ve.Left <- x
                        ve.Right <- q
                        x.Right <- ve
                        q.Left <- ve
                        Node (ve :> Entry<'a>)
                    else 
                        q.Add(r,value)
            elif value < x.Value then 
                match x.Left with 
                | null -> 
                    let ve = ValueEntry.Create()
                    ve.Valuev <- value
                    ve.Right <- x
                    x.Left <- ve
                    Node (ve :> Entry<'a>)
                | q -> 
                    if q.Value < value then
                        let ve = ValueEntry.Create() 
                        ve.Valuev <- value
                        ve.Right <- x
                        ve.Left <- q
                        x.Left <- ve
                        q.Right <- ve
                        Node (ve :> Entry<'a>)
                    else 
                        q.Add(r,value)
            else
                count <- count + 1
                NoOp
        override x.Contains(value) = 
            if value = x.Value then 
                true
            elif value > x.Value then 
                match x.Right with 
                | null -> false
                | x -> x.Contains(value)
            else
                match x.Left with 
                | null -> false
                | x -> x.Contains(value)
        override x.Remove(value) = 
            if value = x.Value then 
                if count = 1 then 
                    count <- 0
                    match x.Left,x.Right with 
                    | null,null -> ()
                    | l, null -> l.Right <- null
                    | null,r -> r.Left <- null
                    | l,r -> l.Right <- r; r.Left <- l
                    del x
                    true
                else 
                    count <- count - 1 
                    false
            elif value > x.Value then 
                match x.Right with 
                | null -> false
                | x -> x.Remove(value)
            else
                match x.Left with 
                | null -> false
                | x -> x.Remove(value)
     
    [<AbstractClass>]
    type Level<'a when 'a : comparison>() = 
        inherit Entry<'a>()
        override x.Value = x.Down.Value
        override x.Count = x.Down.Count

    type SkipLevel<'a when 'a : comparison>() = 
        inherit Level<'a>()
        static let pool = ConcurrentQueue()
        static let del (o : SkipLevel<'a>) = 
            if pool.Count < SkipLevel<'a>.PoolSize then 
                o.Reset()
                pool.Enqueue o
        let mutable count = 1
        member x.Reset() = 
            count <- 1 
            x.Width <- 1
            x.Left <- null
            x.Right <- null
            x.Up <- null
            x.Down <- null
        static member Create<'a>() : SkipLevel<'a> = 
            let scc,v = pool.TryDequeue()
            if scc then v else SkipLevel()
        static member val PoolSize = 16
        override x.UpdateWidth() = 
            match x.Right with 
            | null -> 
                let mutable d = x.Down
                let mutable w = 0
                while not(isNull d) do 
                    w <- d.Width + w
                    d <- d.Right
                x.Width <- w
            | r -> 
                let v = r.Value
                let mutable d = x.Down
                let mutable w = 0
                while d.Value <> v do 
                    w <- d.Width + w
                    d <- d.Right
                x.Width <- w
        override x.Max = 
            match x.Right with 
            | null -> 
                let mutable r = x :> Entry<_>
                while not(isNull r.Down) do 
                    r <- r.Down
                    while not(isNull r.Right) do r <- r.Right
                match r with
                | null -> x.Value 
                | r -> r.Value
            | r -> r.Down.Left.Value
        override x.Min = x.Value
        override x.Add(r, value : 'a) =
            if isNull x.Left || value >= x.Value then 
                if isNull x.Right || value < x.Right.Value then 
                    match x.Down.Add(r,value) with 
                    | Node n -> 
                        x.Width <- x.Width + 1
                        if r() then 
                            if value > x.Value then 
                                let l = SkipLevel.Create()
                                l.Left <- x
                                l.Right <- x.Right
                                l.Down <- n
                                n.Up <- l
                                x.Right <- l
                                l.UpdateWidth()
                                x.UpdateWidth()
                                Node (l :> Entry<'a>)
                            else 
                                let l = SkipLevel.Create()
                                l.Left <- x.Left
                                l.Right <- x
                                l.Down <- n
                                n.Up <- l
                                x.Left <- l
                                l.UpdateWidth()
                                x.UpdateWidth()
                                Node (l :> Entry<'a>)
                        else Added
                    | Added -> 
                        x.Width <- x.Width + 1
                        Added
                    | NoOp -> NoOp
                else x.Right.Add(r,value)
            else x.Left.Add(r,value)
        override x.Contains(value : 'a) = 
            if x.Value = value then 
                true
            elif isNull x.Left || value >= x.Value then 
                if isNull x.Right || value < x.Right.Value then 
                    x.Down.Contains(value)
                else x.Right.Contains(value)
            else x.Left.Contains(value)
        override x.Remove(value : 'a) = 
            if isNull x.Left || value >= x.Value then 
                if isNull x.Right || value < x.Right.Value then 
                    if x.Down.Remove(value) && value = x.Value then 
                        match x.Left,x.Right with 
                        | null,null -> ()
                        | l, null -> l.Right <- null
                        | null,r -> r.Left <- null
                        | l,r -> 
                            l.Right <- r
                            r.Left <- l
                            l.UpdateWidth()
                            r.UpdateWidth()
                        del x
                        true
                    else
                        false
                else x.Right.Remove(value)
            else x.Left.Remove(value)
            
    [<AutoOpen>]
    module internal LeftRight = 
        let left x =
            let mutable left = x :> _ Entry
            while not(isNull left.Left) do left <- left.Left
            left
        let right x =
            let mutable right = x :> _ Entry
            while not(isNull right.Right) do right <- right.Right
            right

    type TopLevel<'a when 'a : comparison>(p,maxLevel,seed) = 
        inherit Entry<'a>()
        let levels = ResizeArray()
        let rng = Random(seed)
        override x.UpdateWidth() = ()
        override x.Add(r, value : 'a) = 
            if levels.Count = 0 then 
                x.Width <- 1
                let e = ValueEntry.Create() 
                e.Valuev <- value
                levels.Add(e :> Entry<'a>)
            else
                let lastl = levels.[levels.Count - 1]
                match lastl.Add(r,value) with 
                | Node(n) -> 
                    x.Width <- x.Width + 1
                    if levels.Count < maxLevel && r() then 
                        let i2 = SkipLevel.Create() 
                        i2.Down <- n
                        n.Up <- i2
                        levels.Add i2
                | Added -> 
                    x.Width <- x.Width + 1
                | NoOp -> ()
            NoOp
        member x.Add(value) = x.Add((fun () -> rng.NextDouble() < p), value) |> ignore
        override x.Value = levels.[levels.Count - 1].Value
        override x.Count = (left levels.[levels.Count - 1]).Count
        override x.Max = (right levels.[levels.Count - 1]).Max
        override x.Min = 
            let mutable r = left levels.[levels.Count - 1]
            while not(isNull r.Down) do 
                r <- r.Down
                while not(isNull r.Left) do r <- r.Left
            match r with
            | null -> x.Value 
            | r -> r.Value
        member x.Levels = levels      
        member x.Clear() = levels.Clear()
        override x.Contains(value) = 
            if levels.Count = 0 then 
                false
            else
                let lastl = levels.[levels.Count - 1]
                lastl.Contains(value)
        override x.Remove(value) = 
            if levels.Count = 0 then 
                false
            else
                let lastl = levels.[levels.Count - 1]
                lastl.Remove(value)
        member x.Item 
            with get(i : int) = 
                let mutable n = levels.[0] |> left
                if i = 0 then n.Value else
                let mutable cw = 0
                let mutable lvl = 0
                while cw < i do 
                    match n.Up, n.Right with 
                    | null, null -> 
                        n <- n.Down
                    | u, _ when not(isNull u) && u.Width + cw <= i -> 
                        lvl <- lvl + 1
                        n <- u
                        while not(isNull n.Up) && n.Up.Width + cw <= i do 
                            lvl <- lvl + 1
                            n <- n.Up
                    | _, null -> cw <- i + 1
                    | _, r -> 
                        while cw + n.Width > i do
                            lvl <- lvl - 1
                            n <- n.Down
                        while isNull(n.Right) do 
                            lvl <- lvl - 1
                            n <- n.Down
                        cw <- n.Width + cw
                        n <- n.Right
                n.Value

open SkipList

type SkipList<'a when 'a : comparison>(?p,?maxLevel,?seed) = 
    let p = defaultArg p 0.5
    let maxLevel = defaultArg maxLevel 100
    let seed = defaultArg seed 0
    let tl = TopLevel<'a>(p,maxLevel,seed)
    member x.Probability = p
    member x.MaxLevel = maxLevel
    member x.Seed = seed
    member x.Max = tl.Max
    member x.Min = tl.Min
    member x.StringRepr() = 
        tl.Levels
        |> Seq.map 
            (fun x ->
                let sb = System.Text.StringBuilder()
                let x = left x
                ()
            )
        |> Seq.iter ignore
    member this.Add(item: 'a): unit = tl.Add(item)
    member this.Clear(): unit = tl.Clear()
    member this.Contains(item: 'a): bool = tl.Contains(item)
    member this.CopyTo(array: 'a [], arrayIndex: int): unit = 
        for i = 0 to this.Count - 1 do
            array.[i + arrayIndex] <- this.[i]
    member this.Count: int = tl.Width
    member this.IndexOf(item: 'a): int = 
        raise (System.NotImplementedException())
    member this.Insert(index: int, item: 'a): unit = raise (System.InvalidOperationException())
    member this.IsReadOnly: bool = false
    member this.Item
        with get (index: int): 'a = tl.[index]
        and set (index: int) (v: 'a): unit = raise (System.InvalidOperationException())
    member this.Remove(item: 'a): bool = tl.Remove(item)
    member this.RemoveAt(index: int): unit = 
        raise (System.NotImplementedException())
    interface IList<'a> with 
         member this.Add(item: 'a): unit = this.Add(item)
         member this.Clear(): unit = this.Clear()
         member this.Contains(item: 'a): bool = this.Contains(item)
         member this.CopyTo(array: 'a [], arrayIndex: int): unit = this.CopyTo(array, arrayIndex)
         member this.Count: int = this.Count
         member this.GetEnumerator(): Collections.IEnumerator = 
             raise (System.NotImplementedException())
         member this.GetEnumerator(): IEnumerator<'a> = 
             raise (System.NotImplementedException())
         member this.IndexOf(item: 'a): int = this.IndexOf(item)
         member this.Insert(index: int, item: 'a): unit = this.Insert(index, item)
         member this.IsReadOnly: bool = this.IsReadOnly
         member this.Item
             with get (index: int): 'a = this.[index]
             and set (index: int) (v: 'a): unit = this.[index] <- v
         member this.Remove(item: 'a): bool = this.Remove(item)
         member this.RemoveAt(index: int): unit = this.RemoveAt(index)

type SkipListRunning(length,p,maxLevel,seed) = 
    let tl = TopLevel(p,maxLevel,seed)
    let q = Queue<double>()
    let dlen = double length
    let mutable mx = Double.MinValue
    let mutable mn = Double.MaxValue
    let mutable mu = 0.0
    member x.Add(value : double) = 
        mx <- max value mx
        mn <- min value mn
        q.Enqueue(value)
        if q.Count > length then 
            let exiting = q.Dequeue()
            mu <- (mu*dlen - exiting + value) / dlen
            if tl.Remove(exiting) then 
                if mx = exiting then 
                    mx <- tl.Max
                if mn = exiting then 
                    mn <- tl.Min
        else
            mu <- (mu*double(q.Count - 1) + value) / double q.Count
    member x.Count = q.Count
          




