namespace Trebuchet
#nowarn "9"

open System.Numerics
open System.Runtime.CompilerServices

module V = 
    let inline reduce init vacc vtoa arrAcc (a : ^a []) = 
        let sz = Vector< ^a>.Count
        let mutable acc = init Vector< ^a>.Zero
        let mutable i = 0
        while i <= a.Length - sz do 
            let v = Vector(a,i)
            acc <- vacc acc v
            i <- i + sz
        let mutable result = vtoa acc
        while i < a.Length do 
            result <- arrAcc result a.[i]
            i <- i + 1
        result
    let inline sum (a : ^a []) = 
        reduce id (fun a b -> Vector.Add(a,b)) (fun a -> Vector.Dot(a,Vector.One)) (+) a
    let inline binaryOp vop sop (r : ^b []) (a : ^a []) (b : ^a []) = 
        let sz = Vector< ^a>.Count
        let mutable i = 0
        while i <= a.Length - sz do 
            let av = Vector(a,i)
            let bv = Vector(b,i)
            let cv : ^b Vector = vop av bv
            cv.CopyTo(r,i)
            i <- i + sz
        while i < a.Length do 
            r.[i] <- sop a.[i] b.[i]
            i <- i + 1
        r
    let inline unaryOp vop sop (r : ^a []) (a : ^a []) = 
        let sz = Vector< ^a>.Count
        let mutable i = 0
        while i <= a.Length - sz do 
            let av = Vector(a,i)
            let cv : _ Vector = vop av
            cv.CopyTo(r,i)
            i <- i + sz
        while i < a.Length do 
            r.[i] <- sop a.[i]
            i <- i + 1
        r
    let inline add (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.Add(a,b)) (+) r a b
    let inline mul (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.Multiply(a,b)) ( * ) r a b
    let inline sub (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.Subtract(a,b)) (-) r a b
    let inline div (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.Divide(a,b)) (/) r a b
    let inline abs (r : _ []) (a : _ []) = unaryOp (fun a -> Vector.Abs(a)) (abs) r a
    let inline negate (r : _ []) (a : _ []) = unaryOp (fun a -> Vector.Negate(a)) (~-) r a
    let inline sqrt (r : _ []) (a : _ []) = unaryOp (fun a -> Vector.SquareRoot (a)) (sqrt) r a
    let inline maximum (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.Max(a,b)) (max) r a b
    let inline minimum (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.Min(a,b)) (min) r a b
    let inline bl f a b = if f a b then LanguagePrimitives.GenericOne<_> else LanguagePrimitives.GenericZero<_>
    let inline gt (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.GreaterThan<_>(a,b)) (bl(>)) r a b
    let inline gte (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.GreaterThanOrEqual<_>(a,b)) (bl(>=)) r a b
    let inline lt (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.LessThan<_>(a,b)) (bl(<)) r a b
    let inline lte (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.LessThanOrEqual<_>(a,b)) (bl(<=)) r a b
    let inline eq (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.Equals<_>(a,b)) (bl(=)) r a b
    let inline bitwiseOr (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.BitwiseOr<_>(a,b)) (|||) r a b
    let inline bitwiseAnd (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.BitwiseAnd<_>(a,b)) (&&&) r a b
    let inline xor (r : _ []) (a : _ []) (b : _ []) = binaryOp (fun a b -> Vector.Xor<_>(a,b)) (^^^) r a b
    

module UnsafeVector = 
    open Trebuchet.Collections
    open System.Runtime.InteropServices

    let inline reduce init vacc vtoa arrAcc (a : ^a MemArray) = 
        let sz = Vector< ^a>.Count
        let mutable acc = init Vector< ^a>.Zero
        let mutable i = 0
        let s = MemoryMarshal.Cast< ^a,Vector< ^a>>(System.Span< ^a>(a.NativePtr |> NativeInterop.NativePtr.toVoidPtr, a.Length))
        while i < s.Length do 
            acc <- vacc acc s.[i]
            i <- i + sz
        let mutable result = vtoa acc
        i <- i * sz
        while i < a.Length do 
            result <- arrAcc result a.[i]
            i <- i + 1
        result
    let inline sum (a : ^a MemArray) = 
        reduce id (fun a b -> Vector.Add(a,b)) (fun a -> Vector.Dot(a,Vector.One)) (+) a
    let inline binaryOp vop sop (r : ^a MemArray) (a : ^a MemArray) (b : ^a MemArray) = 
        let sz = Vector< ^a>.Count
        let mutable i = 0
        let sa = MemoryMarshal.Cast< ^a,Vector< ^a>>(System.Span< ^a>(a.NativePtr |> NativeInterop.NativePtr.toVoidPtr, a.Length))
        let sb = MemoryMarshal.Cast< ^a,Vector< ^a>>(System.Span< ^a>(b.NativePtr |> NativeInterop.NativePtr.toVoidPtr, b.Length))
        let sr = MemoryMarshal.Cast< ^a,Vector< ^a>>(System.Span< ^a>(r.NativePtr |> NativeInterop.NativePtr.toVoidPtr, r.Length))
        while i < a.Length - sz do 
            sr.[i] <- vop sa.[i] sb.[i]
            i <- i + sz
        i <- i * sz
        while i < a.Length do 
            r.Set(i,sop a.[i] b.[i])
            i <- i + 1
        r
    let inline unaryOp vop sop (r : ^a MemArray) (a : ^a MemArray) = 
        let sz = Vector< ^a>.Count
        let mutable i = 0
        let sa = MemoryMarshal.Cast< ^a,Vector< ^a>>(System.Span< ^a>(a.NativePtr |> NativeInterop.NativePtr.toVoidPtr, a.Length))
        let sr = MemoryMarshal.Cast< ^a,Vector< ^a>>(System.Span< ^a>(r.NativePtr |> NativeInterop.NativePtr.toVoidPtr, r.Length))
        while i <= a.Length - sz do 
            sr.[i] <- vop sa.[i]
            i <- i + sz
        i <- i * sz
        while i < a.Length do 
            r.Set(i,sop a.[i])
            i <- i + 1
        r
    let inline add (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.Add(a,b)) (+) r a b
    let inline mul (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.Multiply(a,b)) ( * ) r a b
    let inline sub (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.Subtract(a,b)) (-) r a b
    let inline div (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.Divide(a,b)) (/) r a b
    let inline abs (r : _ MemArray) (a : _ MemArray) = unaryOp (fun a -> Vector.Abs(a)) (abs) r a
    let inline negate (r : _ MemArray) (a : _ MemArray) = unaryOp (fun a -> Vector.Negate(a)) (~-) r a
    let inline sqrt (r : _ MemArray) (a : _ MemArray) = unaryOp (fun a -> Vector.SquareRoot (a)) (sqrt) r a
    let inline maximum (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.Max(a,b)) (max) r a b
    let inline minimum (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.Min(a,b)) (min) r a b
    let inline gt (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.GreaterThan<_>(a,b)) (>) r a b
    let inline gte (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.GreaterThanOrEqual<_>(a,b)) (>=) r a b
    let inline lt (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.LessThan<_>(a,b)) (<) r a b
    let inline lte (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.LessThanOrEqual<_>(a,b)) (<=) r a b
    let inline eq (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.Equals<_>(a,b)) (=) r a b
    let inline bitwiseOr (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.BitwiseOr<_>(a,b)) (|||) r a b
    let inline bitwiseAnd (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.BitwiseAnd<_>(a,b)) (&&&) r a b
    let inline xor (r : _ MemArray) (a : _ MemArray) (b : _ MemArray) = binaryOp (fun a b -> Vector.Xor<_>(a,b)) (^^^) r a b

module SpanVector = 
    open Trebuchet.Collections
    open System
    open System.Runtime.InteropServices
    open System

    let inline reduce init vacc vtoa arrAcc (a : ^a ReadOnlySpan) = 
        let sz = Vector< ^a>.Count
        let mutable acc = init Vector< ^a>.Zero
        let mutable i = 0
        let s = MemoryMarshal.Cast< ^a,Vector< ^a>>(a)
        while i < s.Length do 
            acc <- vacc acc s.[i]
            i <- i + sz
        let mutable result = vtoa acc
        i <- i * sz
        while i < a.Length do 
            result <- arrAcc result a.[i]
            i <- i + 1
        result
    let inline sum (a : ^a ReadOnlySpan) = 
        reduce id (fun a b -> Vector.Add(a,b)) (fun a -> Vector.Dot(a,Vector.One)) (+) a
    let inline binaryOp vop sop (r : ^a Span) (a : ^a ReadOnlySpan) (b : ^a ReadOnlySpan) = 
        let sz = Vector< ^a>.Count
        let mutable i = 0
        let sa = MemoryMarshal.Cast< ^a,Vector< ^a>>(a)
        let sb = MemoryMarshal.Cast< ^a,Vector< ^a>>(b)
        let sr = MemoryMarshal.Cast< ^a,Vector< ^a>>(r)
        while i < a.Length - sz do 
            sr.[i] <- vop sa.[i] sb.[i]
            i <- i + sz
        i <- i * sz
        while i < a.Length do 
            r.[i] <- sop a.[i] b.[i]
            i <- i + 1
        r
    let inline unaryOp vop sop (r : ^a Span) (a : ^a ReadOnlySpan) = 
        let sz = Vector< ^a>.Count
        let mutable i = 0
        let sa = MemoryMarshal.Cast< ^a,Vector< ^a>>(a)
        let sr = MemoryMarshal.Cast< ^a,Vector< ^a>>(r)
        while i <= a.Length - sz do 
            sr.[i] <- vop sa.[i]
            i <- i + sz
        i <- i * sz
        while i < a.Length do 
            r.[i] <- sop a.[i]
            i <- i + 1
        r
    let inline add (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.Add(a,b)) (+) r a b
    let inline mul (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.Multiply(a,b)) ( * ) r a b
    let inline sub (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.Subtract(a,b)) (-) r a b
    let inline div (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.Divide(a,b)) (/) r a b
    let inline abs (r : _ Span) (a : _ ReadOnlySpan) = unaryOp (fun a -> Vector.Abs(a)) (abs) r a
    let inline negate (r : _ Span) (a : _ ReadOnlySpan) = unaryOp (fun a -> Vector.Negate(a)) (~-) r a
    let inline sqrt (r : _ Span) (a : _ ReadOnlySpan) = unaryOp (fun a -> Vector.SquareRoot (a)) (sqrt) r a
    let inline maximum (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.Max(a,b)) (max) r a b
    let inline minimum (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.Min(a,b)) (min) r a b
    let inline gt (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.GreaterThan<_>(a,b)) (>) r a b
    let inline gte (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.GreaterThanOrEqual<_>(a,b)) (>=) r a b
    let inline lt (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.LessThan<_>(a,b)) (<) r a b
    let inline lte (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.LessThanOrEqual<_>(a,b)) (<=) r a b
    let inline eq (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.Equals<_>(a,b)) (=) r a b
    let inline bitwiseOr (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.BitwiseOr<_>(a,b)) (|||) r a b
    let inline bitwiseAnd (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.BitwiseAnd<_>(a,b)) (&&&) r a b
    let inline xor (r : _ Span) (a : _ ReadOnlySpan) (b : _ ReadOnlySpan) = binaryOp (fun a b -> Vector.Xor<_>(a,b)) (^^^) r a b
open V

module Crap =
    let inline (|V|) a = (^a : (member Array : ^b []) a)
    let inline m a = Array.zeroCreate (Array.length a)
open Crap
[<Struct>]       
type V<'a>(a : 'a []) = 
    member x.Array = a
    member inline x.Length = x.Array.Length
    member inline x.Item(i) = x.Array.[i]
    static member inline (+)(V a,V b) = add (m a) a b |> V
    static member inline (-)(V a,V b) = sub (m a) a b |> V
    static member inline (~-)(V a) = negate (m a) a |> V
    static member inline ( * )(V a, V b) = mul (m a) a b |> V
    static member inline (/)(V a, V b) = div (m a) a b |> V
    static member inline (.>)(V a, V b) = gt (m a) a b |> V
    static member inline (.>=)(V a, V b) = gte (m a) a b |> V
    static member inline (.<)(V a, V b) = lt (m a) a b |> V
    static member inline (.<=)(V a, V b) = lte (m a) a b |> V
    static member inline (.=)(V a, V b) = eq (m a) a b |> V
    static member inline (&&&)(V a, V b) = bitwiseAnd (m a) a b |> V
    static member inline (|||)(V a, V b) = bitwiseOr (m a) a b |> V
    static member inline (^^^)(V a, V b) = xor (m a) a b |> V
    static member inline Max(V a,V b) = maximum (m a) a b |> V
    static member inline Min(V a,V b) = minimum (m a) a b |> V
    static member inline Abs(V a) = abs (m a) a |> V
    static member inline Sqrt(V a) = sqrt (m a) a |> V
