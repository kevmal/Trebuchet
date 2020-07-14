open System.Runtime.CompilerServices
#r @"D:\AppData\nuget\packages\system.numerics.vectors\4.5.0\lib\net46\System.Numerics.Vectors.dll"
#load @"E:\profile\fsi\.\packagegrp\23B9A90C5AADCEF630733F70F9F5C13F1E4C24E41F973897F2A1BC95AC8F49A2\.paket\load\main.group.fsx"


open FASTER


open System.Numerics
open FSharp.Core.LanguagePrimitives

IntrinsicFunctions.GetArray

open System
let a = "a"
String.IsInterned a
String.Intern


Vector<int64>.Count

let inline vtype< ^a when ^a : struct> = ()
[<Extension>]
type Ext() = 
    [<Extension>]
    static member inline (?)(f : ^a -> ^b, a : ^a []) = 
        //vtype<'b>
        let c = Array.zeroCreate a.Length
        for i = 0 to a.Length - 1 do
            c.[i] <- f a.[i]
        c
    [<Extension>]
    static member inline B(f : ^a -> ^b, a : ^a []) = 
        //vtype<'b>
        let c = Array.zeroCreate a.Length
        for i = 0 to a.Length - 1 do
            c.[i] <- f a.[i]
        c
    [<Extension>]
    static member inline B(f : ^a * ^b -> ^c, a : ^a []) = 
        fun b -> 
            let c = Array.zeroCreate a.Length
            for i = 0 to a.Length - 1 do
                c.[i] <- f (a.[i], b)
            c
    [<Extension>]
    static member inline B(f : ^a * ^b * ^d -> ^c, a : ^a []) = 
        fun b d -> 
            let c = Array.zeroCreate a.Length
            for i = 0 to a.Length - 1 do
                c.[i] <- f (a.[i], b, d)
            c
                

open System
let a = [|1.0 .. 10.0|]
Math.Atan2.B(a) 34.0

type Poo = AA with 
    static member A(a,b,c) = a + b + c + 0.0

Poo.A.B a 1.0 2.0


(Math.Sqrt).B(a)
Math.Sqrt.B(a)
