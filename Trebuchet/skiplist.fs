namespace Trebuchet.DataStructures




open System
open System.IO
open System.Collections.Concurrent
open System.Collections.Generic



module SkipList =



    type [<AllowNullLiteral>] Entry<'a when 'a : comparison>() = 
        static let pool = ConcurrentQueue()
        static let del (o : Entry<'a>) = 
            if pool.Count < Entry<'a>.PoolSize then 
                o.Reset()
                pool.Enqueue o
        static member val PoolSize = 16 with get,set
        member val Left : Entry<'a> = null with get,set
        member val Right : Entry<'a> = null with get,set
        member val Down : Entry<'a> = null with get,set
        member val Up : Entry<'a> = null with get,set
        member val Width = 1 with get,set
        member val Value : 'a = Unchecked.defaultof<_> with get,set
        member val Count = 1 with get,set
        override x.ToString() : string = 
            let left = if isNull (box x.Left) then "_" else x.Left.Value.ToString()
            let right =  if isNull (box x.Right) then "_" else x.Right.Value.ToString()
            let up =  if isNull (box x.Up) then "_" else x.Up.Value.ToString()
            sprintf "%s -- %s -- %s (up %A) (down %A)" left (x.Value.ToString()) right up (isNull (box x.Down))
        member x.Reset() = 
            x.Left <- null
            x.Right <- null
            x.Down <- null
            x.Up <- null
            x.Width <- 1
            x.Count <- 1
            x.Value <- Unchecked.defaultof<_>
        member x.Delete() = del x
        static member Create<'a>(value : 'a) = 
            let scc,v = pool.TryDequeue()
            let v = if scc then v else Entry()
            v.Value <- value 
            v

    let up x =
        let mutable up = x :> _ Entry
        while not(isNull up.Up) do up <- up.Up
        up

    let down x =
        let mutable down = x :> _ Entry
        while not(isNull down.Down) do down <- down.Down
        down
  
    let left x =
        let mutable left = x :> _ Entry
        while not(isNull left.Left) do left <- left.Left
        left

    let right x =
        let mutable right = x :> _ Entry
        while not(isNull right.Right) do right <- right.Right
        right

    let findLesserOrEq (v : 'a) (n : Entry<'a>) =
        let mutable e = n
        while not(isNull(e.Up)) do 
            e <- e.Up
        while not(isNull(e.Left)) && e.Left.Value >= v do e <- e.Left
        while not(isNull(e.Right)) && e.Right.Value < v do e <- e.Right
        while not(isNull(e.Down)) do
            e <- e.Down
            while not(isNull(e.Right)) && e.Right.Value <= v do e <- e.Right
        e

    let inline onDirectRight (interval : Entry<'a>) (n : Entry<'a>) = 
        interval.Value <= n.Value && (isNull interval.Right || interval.Right.Value > n.Value)

    let findLesserOrEqOnLvl (v : 'a) (n : Entry<'a>) =
        let mutable lvl = 0
        let mutable e = n
        while not(isNull(e.Up)) && not(onDirectRight e n) do 
            e <- e.Up
            lvl <- lvl + 1
        while not(isNull(e.Left)) && e.Left.Value >= v do e <- e.Left
        while not(isNull(e.Right)) && e.Right.Value < v do e <- e.Right
        while lvl > 0 do
            e <- e.Down
            lvl <- lvl - 1
            while not(isNull(e.Right)) && e.Right.Value <= v do e <- e.Right
        e


    let realignParent (n : Entry<'a>) = 
        let check (e : Entry<'a>) = 
            assert (isNull e.Left || e.Left.Value < e.Value)
            assert (isNull e.Right || e.Right.Value > e.Value)
            assert (isNull e.Down || Object.ReferenceEquals(e.Down.Up, e))
            e
        if isNull n.Up then 
            n
        else    
            let newp = findLesserOrEqOnLvl n.Value n.Up
            n.Up <- newp
            check n.Up |> ignore
            check n

    let checkAlignedParent (n : Entry<'a>) = 
        let p = n.Up
        realignParent n |> ignore
        assert(LanguagePrimitives.PhysicalEquality p n.Up)
           

    let calcCount (n : Entry<'a>) =
        let mutable e = if isNull n.Down then n else n.Down
        let mutable c = 0
        if isNull n.Right then 
            while not(isNull e) do
                c <- e.Count + c
                e <- e.Right
        else
            let rv = n.Right.Value
            while e.Value <> rv do
                c <- e.Count + c
                e <- e.Right
        c

    let recalcUp (e:Entry<'a>) =
        let mutable p = e
        while not(isNull p.Up) do
            checkAlignedParent p |> ignore
            p.Up.Count <- calcCount p.Up
            p <- p.Up

    let checkCount (n : Entry<'a>) =
        if isNull n then () else
        let count = 
            if isNull n.Right then 
                let mutable e = down n
                let mutable c = 0
                while not(isNull e) do
                    c <- e.Count + c
                    e <- e.Right
                c
            else
                let mutable e = down n
                let mutable c = 0
                while e.Value <> n.Right.Value do
                    c <- e.Count + c
                    e <- e.Right
                c
        assert(count = n.Count)            

    let add (v : 'a) (n : Entry<'a>) = 
        let check (e : Entry<'a>) = 
            assert (isNull e.Left || e.Left.Value < e.Value)
            assert (isNull e.Right || e.Right.Value > e.Value)
            assert (isNull e.Down || Object.ReferenceEquals(e.Down.Up, e))
            e
        let rec loop (n : Entry<'a>) lvl = 
            if v > n.Value then 
                if isNull n.Right then 
                    if isNull n.Down then
                        let entry = Entry.Create(v)
                        entry.Left <- n
                        entry.Up <- n.Up
                        n.Right <- entry
                        check n |> ignore
                        check entry
                    else    
                        loop (n.Down) (lvl + 1)
                elif n.Right.Value > v then 
                    if isNull n.Down then 
                        let entry = Entry.Create(v)
                        entry.Up <- n.Up
                        let r = n.Right
                        n.Right <- entry
                        r.Left <- entry
                        entry.Left <- n
                        entry.Right <- r
                        check r |> ignore
                        check n |> ignore
                        check entry
                    else    
                        loop (n.Down) (lvl + 1)
                else 
                    loop n.Right lvl
            elif v = n.Value then 
                let n = down n
                n.Count <- n.Count + 1
                n
                //let mutable down = n
                //down.Count <- down.Count + 1
                //while not(isNull down.Down) do 
                //    down <- down.Down
                //    down.Count <- down.Count + 1
                //down
            else
                if isNull n.Left then 
                    if isNull n.Down then 
                        let entry = Entry.Create(v)
                        entry.Right <- n
                        entry.Up <- n.Up
                        n.Left <- entry
                        check n |> ignore
                        check entry
                    else    
                        loop (n.Down) (lvl + 1)
                elif n.Left.Value < v then 
                    if isNull n.Down then 
                        let entry = Entry.Create(v)
                        entry.Up <- n.Up
                        let l = n.Left
                        n.Left <- entry
                        l.Right <- entry
                        entry.Right <- n
                        entry.Left <- l
                        check l |> ignore
                        check n |> ignore
                        check entry
                    else
                        loop (n.Left.Down) (lvl + 1)
                else 
                    loop n.Left lvl
        let e = loop n 0 |> check 
        recalcUp e
        e

    let totalCount (n : Entry<'a>) = 
        let mutable e = left(up n)
        let mutable c = 0
        while not(isNull e) do 
            c <- e.Count + c
            e <- e.Right
        c
    let addWithPromote (r : Random) (p : double) maxlevel (v : 'a) (n : Entry<'a>) = 
        let entry = add v n
        let mutable e = entry
        let mutable lvl = 0
        let check (e : Entry<'a>) = 
            assert (isNull e || isNull e.Left || e.Left.Value < e.Value)
            assert (isNull e || isNull e.Right || e.Right.Value > e.Value)
            assert (isNull e || isNull e.Down || Object.ReferenceEquals(e.Down.Up, e))
            e
        // Far left will always have a node on all levels
        if not(isNull e.Up) then
            if isNull e.Left then 
                if not(isNull e.Right) then 
                    let mutable u = e.Up
                    let mutable laste = e
                    while not(isNull u)  do 
                        if u.Value <> e.Value then 
                            u.Value <- e.Value
                            u.Down <- laste
                            u.Count <- u.Count + laste.Count
                            checkCount u
                        laste <- u
                        u <- u.Up
        else
            while not(isNull e) && r.NextDouble() < p do
                lvl <- lvl + 1
                if not (isNull e.Up) then 
                    if e.Up.Value = e.Value then 
                        e <- e.Up
                    elif r.NextDouble() < p then 
                        // promote
                        let parent = Entry.Create(v)
                        checkAlignedParent e.Up
                        parent.Up <- e.Up.Up
                        parent.Down <- e
                        let u = findLesserOrEqOnLvl e.Value e.Up
                        assert(LanguagePrimitives.PhysicalEquality u e.Up)  //TODO: If this is okay replace u
                        checkCount u
                        if u.Value < e.Value then 
                            parent.Left <- u
                            parent.Right <- u.Right
                            u.Right <- parent
                            if not(isNull parent.Right) then 
                                parent.Right.Left <- parent
                            e.Up <- parent
                            parent.Count <- calcCount parent
                            u.Count <- u.Count - parent.Count
                            checkCount parent
                            checkCount u
                            check parent |> ignore
                        else    
                            parent.Right <- u
                            parent.Left <- u.Left
                            u.Left <- parent
                            if not(isNull parent.Left) then 
                                parent.Left.Right <- parent
                            e.Up <- parent
                            parent.Count <- calcCount parent
                            if not(isNull parent.Left) then 
                                parent.Left.Count <- parent.Left.Count - parent.Count
                            checkCount parent
                            checkCount parent.Left
                            checkCount u
                            checkCount u.Left
                            checkCount u.Right
                            check parent |> ignore
                        do  // Update 'Up' down and right till next 'parent'
                            let mutable c = e.Right
                            while not (isNull c) && not(LanguagePrimitives.PhysicalEquality c.Up parent.Right)  do 
                                c.Up <- parent
                                checkAlignedParent c
                                c <- c.Right
                        checkAlignedParent parent
                        checkAlignedParent e
                        e <- check parent
                    else    
                        e <- null
                elif lvl < maxlevel then 
                    //new level
                    // Create Up on far left
                    let l = left e
                    let lp = Entry.Create(l.Value)
                    lp.Down <- l
                    assert(isNull lp.Up)
                    l.Up <- lp
                    lp.Count <- l.Count
                    let mutable nn = l.Right
                    while not(LanguagePrimitives.PhysicalEquality nn e) do 
                        lp.Count <- lp.Count + nn.Count
                        nn.Up <- lp
                        nn <- check nn.Right
                    let parent = Entry.Create(v)
                    parent.Count <- e.Count
                    assert(isNull parent.Up)
                    e.Up <- parent
                    parent.Down <- e
                    parent.Count <- e.Count
                    nn <- check e.Right
                    while not(isNull nn) do 
                        parent.Count <- parent.Count + nn.Count
                        assert(isNull nn.Up)
                        nn.Up <- parent
                        nn <- check nn.Right
                    //parent.Count <- calcCount parent
                    parent.Left <- lp
                    lp.Right <- parent
                    checkCount lp
                    checkCount parent 
                else    
                    e <- null
        check entry
        
    
    let remove (v : 'a) (n : Entry<'a>) = 
        let mutable e = up n
        let mutable c = -1
        while not(isNull e.Left) && e.Left.Value >= v do 
            e <- e.Left
        while not(isNull e) do
            if e.Value = v then 
                while not(isNull e.Up) && e.Up.Value = e.Value do e <- e.Up
                if e.Count = 1 then 
                    let mutable left = e
                    c <- 0
                    while not(isNull e) do
                        e.Left.Right <- e.Right
                        e.Right.Left <- e.Left
                        left <- e.Left
                        let down = e.Down
                        e.Delete()
                        e <- down
                    if not (isNull left) then recalcUp left
                else    
                    let mutable last = e
                    while not(isNull e) do
                        e.Count <- e.Count - 1
                        c <- e.Count
                        last <- e
                        e <- e.Down
                    recalcUp last
            elif e.Value < v then 
                if isNull e.Right || e.Right.Value > v then 
                    e <- e.Down
                else    
                    e <- e.Right
            else
                if isNull e.Left || e.Left.Value < v then 
                    e <- e.Down
                else    
                    e <- e.Left
        c                        
                    
    let valueCount (v : 'a) (n : Entry<'a>) = 
        let mutable e = up n
        let mutable c = 0
        while not(isNull e.Left) && e.Left.Value >= v do 
            e <- e.Left
        while not(isNull e) do
            if e.Value = v then 
                c <- (down e).Count
                e <- null
            elif e.Value < v then 
                if isNull e.Right || e.Right.Value > v then 
                    e <- e.Down
                else    
                    e <- e.Right
            else
                if isNull e.Left || e.Left.Value < v then 
                    e <- e.Down
                else    
                    e <- e.Left
        c                     

        
    let rec iterNext (n : Entry<'a>) =
        if isNull n.Down then 
            n.Right
        else iterNext n.Down
                    
    let iterStart (n : Entry<'a>) = 
        let mutable e = up n |> left
        while not (isNull e.Down) || not (isNull e.Left) do 
            if isNull e.Left then 
                e <- e.Down
            else    
                e <- e.Left
        e

    let items (n : Entry<'a>) = 
        seq{
            let mutable i = iterStart n
            while not(isNull i) do
                for j = 0 to i.Count - 1 do
                    yield i.Value
                i <- iterNext i
        }
(*       
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
          
*)



