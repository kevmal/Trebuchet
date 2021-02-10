namespace Trebuchet.Collections
#nowarn "9"

open System
open System.Diagnostics
open System.Collections.Generic
open Microsoft.FSharp.Core
open Microsoft.FSharp.Collections
open Microsoft.FSharp.Core.Operators
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Core.LanguagePrimitives.IntrinsicOperators
open System.Runtime.CompilerServices
open System.IO
open Microsoft.FSharp.NativeInterop
open System.IO.MemoryMappedFiles
open System.Runtime.InteropServices

[<AutoOpen>]
module IntPtrExt =             
    type nativeptr<'a when 'a : unmanaged> with 
        member inline x.Item
            with get(i : int) : 'a = 
                NativePtr.get x i
            and set(i : int) v = 
                NativePtr.set x i v
    type IntPtr with 
        member inline x.Item(i : int) : ^a = 
            let xx = NativePtr.ofNativeInt x
            NativePtr.get xx i
        member inline x.Int64 : int64 nativeptr = NativePtr.ofNativeInt x
        member inline x.Int32 : int32 nativeptr = NativePtr.ofNativeInt x
        member inline x.UInt64 : uint64 nativeptr = NativePtr.ofNativeInt x
        member inline x.UInt32 : uint32 nativeptr = NativePtr.ofNativeInt x
        member inline x.Double : double nativeptr = NativePtr.ofNativeInt x
        member inline x.Single : single nativeptr = NativePtr.ofNativeInt x
        member inline x.Float32 : float32 nativeptr = NativePtr.ofNativeInt x
        member inline x.Byte : byte nativeptr = NativePtr.ofNativeInt x
        member inline x.Item with set i v = x.Int64.[i] <- v
        member inline x.Item with set i v = x.Int32.[i] <- v
        member inline x.Item with set i v = x.UInt64.[i] <- v
        member inline x.Item with set i v = x.UInt32.[i] <- v
        member inline x.Item with set i v = x.Double.[i] <- v
        member inline x.Item with set i v = x.Float32.[i] <- v
        member inline x.Item with set i v = x.Byte.[i] <- v

[<Sealed>]
type MemArray<'a when 'a : unmanaged>(mmf : MemoryMappedFile, size : int64, leaveOpen : bool, filename : string option) =
    let mutable disposed = false
    //let sz = int64 l * int64 sizeof<'a>
    //    let fl = FileInfo(filename).Length
    //    match length with 
    //    | Some l -> 
    //        let sz = int64 l * int64 sizeof<'a>
    //        fl |> max sz
    //    | None -> fl
    let length = size / int64(sizeof<'a>) |> int
    //let mmf =
    //    let stream = File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)
    //    MemoryMappedFile.CreateFromFile(stream, null, sz, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, false) 
    let view = mmf.CreateViewAccessor(0L, size)
    let ptr : 'a nativeptr=
        match mmf with 
        | null -> 0n |> NativePtr.ofNativeInt
        | _ -> 
            let mutable ptr = Unchecked.defaultof<_>
            view.SafeMemoryMappedViewHandle.AcquirePointer(&ptr)
            ptr |> NativePtr.toNativeInt |> NativePtr.ofNativeInt
    new(filename, length) = 
        let sz = int64 length*int64 sizeof<'a>
        let stream = File.Open(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)
        let mmf = MemoryMappedFile.CreateFromFile(stream, null, sz, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, false) 
        new MemArray<'a>(mmf, sz, false, Some filename)
    new(filename) = 
        let sz = FileInfo(filename).Length
        let stream = File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)
        let mmf = MemoryMappedFile.CreateFromFile(stream, null, sz, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, false) 
        new MemArray<'a>(mmf, sz, false, Some filename)
    new(length) = 
        let sz = int64 length*int64 sizeof<'a>
        let name = Guid.NewGuid().ToString()
        let mmf = MemoryMappedFile.CreateNew(name,sz) 
        new MemArray<'a>(mmf, sz, false, None)
    static member ReadWrite(filename : string) = new MemArray<'a>(filename)
    static member Read(filename : string) = 
        let sz = FileInfo(filename).Length
        let stream = File.Open(filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
        let mmf = MemoryMappedFile.CreateFromFile(stream, null, sz, MemoryMappedFileAccess.Read, HandleInheritability.None, false) 
        new MemArray<'a>(mmf, sz, false, Some filename)
    member x.NativePtr = ptr
    member inline x.Item(i : int) = NativePtr.get x.NativePtr i
    member inline x.Set(i : int, v) = NativePtr.set x.NativePtr i v
    member x.Length = length
    member x.Dispose(disposing) = 
        if not disposed then 
            if disposing then 
                view.SafeMemoryMappedViewHandle.Dispose()
                view.SafeMemoryMappedViewHandle.ReleasePointer()
                view.Dispose()
                if not leaveOpen then 
                    mmf.Dispose()
        disposed <- true
    member x.ToArray() = Array.init x.Length (fun i -> x.[i])
    member x.Dispose() = 
        x.Dispose(true)
        GC.SuppressFinalize(x)
    interface IDisposable with  
        member x.Dispose() = x.Dispose()
    interface IEnumerable<'a> with 
        member x.GetEnumerator() = (Seq.init x.Length (fun i -> x.[i])).GetEnumerator() :> System.Collections.IEnumerator
        member x.GetEnumerator() = (Seq.init x.Length (fun i -> x.[i])).GetEnumerator()


module MemArrayExtensions = 
    type MemArray<'a when 'a : unmanaged> with 
        member x.Item 
            with set i v = x.Set(i,v)
[<Extension>]
type MemArrayExt() =
    [<Extension>]
    static member inline BinarySearch(x : MemArray<_>, v, startIndex, endIndex) = 
        let mutable si = startIndex
        let mutable ei = endIndex
        while si < ei do
            let m = ((ei - si) >>> 1) + si
            if x.[m] > v then
                ei <- m - 1
            else
                si <- m + 1
        if x.[si] > v then 
            si - 1 
        else si
    [<Extension>]
    static member inline BinarySearch(x : MemArray<_>, v) = x.BinarySearch(v,0,x.Length - 1)
    [<Extension>]
    static member inline Item(x : MemArray<DateTime>, d : DateTime) =
        x.[x.BinarySearch(d)]

[<Sealed>]
type MemResizeArray<'a when 'a : unmanaged>(load : bool, filename : string, capacity : int, maxInc : int64) = 
    let mutable disposed = false
    let mutable i = 0
    let mutable lastIndex = 0
    let mutable size = 0L
    let mutable mmf = 
        let finfo = FileInfo(filename)
        let sz = if  finfo.Exists && finfo.Length > 0L then finfo.Length else int64 capacity * int64 sizeof<'a>
        if load then 
            i <- finfo.Length / int64 sizeof<'a> |> int
        let stream = File.Open(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)
        lastIndex <- sz / int64 sizeof<'a> |> int
        size <- sz
        MemoryMappedFile.CreateFromFile(stream, null, sz, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, false) 
    let mutable view = mmf.CreateViewAccessor(0L, size)
    let mutable ptr : 'a nativeptr=
        let mutable ptr = Unchecked.defaultof<_>
        view.SafeMemoryMappedViewHandle.AcquirePointer(&ptr)
        ptr |> NativePtr.toNativeInt |> NativePtr.ofNativeInt
    static member Create(filename) = new MemResizeArray<'a>(false, filename, 1024*4, 1024L*1000L)
    static member Append(filename) = new MemResizeArray<'a>(true, filename, 1024*4, 1024L*1000L)
    member x.Truncate(length : int) = i <- length
    member x.NativePtr = ptr
    member x.Expand(newCapacity : int64) =
        let newMmf = 
            let stream = File.Open(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)
            lastIndex <- newCapacity / int64 sizeof<'a> |> int
            size <- newCapacity
            //stream.SetLength(size)
            MemoryMappedFile.CreateFromFile(stream, null, size, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, false) 
        let newView = newMmf.CreateViewAccessor(0L, size)
        view.SafeMemoryMappedViewHandle.ReleasePointer()
        view.Dispose()
        mmf.Dispose()
        mmf <- newMmf
        view <- newView
        let mutable p = Unchecked.defaultof<_>
        view.SafeMemoryMappedViewHandle.AcquirePointer(&p)
        ptr <- p |> NativePtr.toNativeInt |> NativePtr.ofNativeInt
    member x.Expand() = 
        let c = size
        let newCapacity = 
            if c > maxInc then 
                c + maxInc
            elif c = 0L then 
                int64 capacity * int64 sizeof<'a>
            else c*2L
        x.Expand(newCapacity)
    member inline x.Item(i : int) = NativePtr.get x.NativePtr i
    member x.Set(index : int, v) = 
        NativePtr.set x.NativePtr i v
    member x.Add(v : 'a) = 
        if i = lastIndex then x.Expand()
        NativePtr.set x.NativePtr i v
        i <- i + 1
    member x.ToMemArray() = new MemArray<'a>(filename)
    member x.Count = i
    member x.SetCount(c) = i <- c
    member x.Dispose(disposing) = 
        if not disposed then 
            if disposing then 
                if not(isNull view) then 
                    view.SafeMemoryMappedViewHandle.Dispose()
                    view.SafeMemoryMappedViewHandle.ReleasePointer()
                    view.Dispose()
                    view <- null
                mmf.Dispose()
                use stream = File.Open(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)
                stream.SetLength(int64 i * int64 sizeof<'a>)
        disposed <- true
    member x.ToArray() = Array.init x.Count (fun i -> x.[i])
    member x.Contains(item : 'a) = 
        let mutable found = false   
        let c = EqualityComparer<'a>.Default
        let mutable j = 0
        while not found && j < x.Count do 
            found <- c.Equals(x.[j], item)
        found
    member x.IndexOf(item : 'a) = 
        let mutable found = false   
        let c = EqualityComparer<'a>.Default
        let mutable j = 0
        while not found && j < x.Count do 
            found <- c.Equals(x.[j], item)
        if found then j else -1
    member x.CopyTo(array : 'a [], arrayIndex : int) = 
        if arrayIndex < 0 || arrayIndex >= array.Length then invalidArg "arrayIndex" (sprintf "arrayIndex of %d not in [0,%d)" arrayIndex array.Length) else 
        use dst = fixed &array.[arrayIndex]
        let bcount = min (array.Length - arrayIndex) x.Count |> uint32
        Unsafe.CopyBlock(NativeInterop.NativePtr.toVoidPtr dst,NativeInterop.NativePtr.toVoidPtr x.NativePtr, bcount)
    member x.Insert(index: int,item: 'a,indexToDrop: int) = 
        if index < indexToDrop then 
            let p = NativePtr.toNativeInt x.NativePtr
            let src = 
                let ptr : 'a nativeptr = p + (nativeint index)*nativeint sizeof<'a> |> NativePtr.ofNativeInt 
                NativePtr.toVoidPtr ptr
            let dst = 
                let ptr : 'a nativeptr = p + (1n + nativeint index)*nativeint sizeof<'a> |> NativePtr.ofNativeInt 
                NativePtr.toVoidPtr ptr
            Unsafe.CopyBlock(dst,src,uint32(indexToDrop - index))
        else 
            let p = NativePtr.toNativeInt x.NativePtr
            let dst = 
                let ptr : 'a nativeptr = p + (nativeint indexToDrop)*nativeint sizeof<'a> |> NativePtr.ofNativeInt 
                NativePtr.toVoidPtr ptr
            let src = 
                let ptr : 'a nativeptr = p + (1n + nativeint indexToDrop)*nativeint sizeof<'a> |> NativePtr.ofNativeInt 
                NativePtr.toVoidPtr ptr
            Unsafe.CopyBlock(dst,src,uint32(index - indexToDrop))
        x.Set(index, item)
    member x.Insert(index,item) = 
        x.Add(Unchecked.defaultof<_>)
        x.Insert(index,item,i-1)
    member x.RemoveAt(index: int)  = 
        let p = NativePtr.toNativeInt x.NativePtr
        let dst = 
            let ptr : 'a nativeptr = p + (nativeint index)*nativeint sizeof<'a> |> NativePtr.ofNativeInt 
            NativePtr.toVoidPtr ptr
        let src = 
            let ptr : 'a nativeptr = p + (1n + nativeint index)*nativeint sizeof<'a> |> NativePtr.ofNativeInt 
            NativePtr.toVoidPtr ptr
        Unsafe.CopyBlock(dst,src,uint32(i - index))
        i <- i - 1
    member x.Remove(item: 'a) = 
        let mutable j = 0 
        let mutable k = 0 
        let comp = EqualityComparer<'a>.Default
        while j < i do 
            if comp.Equals(x.[j], item) then 
                j <- j + 1
            else 
                if k < j then 
                    x.Set(k, x.[j])
                j <- j + 1
                k <- k + 1
        i <- k + 1
        j <> k
    member x.Clear() = i <- 0
    member x.Dispose() = 
        x.Dispose(true)
        GC.SuppressFinalize(x)
    interface IDisposable with  
        member x.Dispose() = x.Dispose()
    interface IList<'a> with
        member this.Add(item: 'a): unit = this.Add item
        member this.Clear(): unit = this.Clear()
        member this.Contains(item: 'a): bool = this.Contains(item)
        member this.CopyTo(array: 'a [], arrayIndex: int): unit = this.CopyTo(array, arrayIndex)
        member this.Count: int = this.Count
        member x.GetEnumerator() = (Seq.init x.Count (fun i -> x.[i])).GetEnumerator() :> System.Collections.IEnumerator
        member x.GetEnumerator() = (Seq.init x.Count (fun i -> x.[i])).GetEnumerator()
        member this.IndexOf(item: 'a): int = this.IndexOf(item)
        member this.Insert(index: int, item: 'a): unit = this.Insert(index, item, 0) |> ignore
        member this.IsReadOnly: bool = false
        member this.Item
            with get (index: int): 'a = 
                this.[index]
            and set (index: int) (v: 'a): unit = 
                this.Set(index, v)
        member this.Remove(item: 'a): bool = this.Remove(item)
        member this.RemoveAt(index: int): unit = this.RemoveAt(index)

            

(*
/// Basic operations on arrays
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
[<RequireQualifiedAccess>]
module MemArray = 

    let inline checkNonNull argName arg =
        if isNull arg then
            nullArg argName

    let inline indexNotFound() = raise (KeyNotFoundException("key not found"))
    
    [<CompiledName("Length")>]
    let length (array: _ MemArray)    = array.Length
    
    [<CompiledName("Last")>]
    let inline last (array: 'T MemArray) =
        if array.Length = 0 then invalidArg "array" "MemArray is empty"
        array.[array.Length-1]

    [<CompiledName("TryLast")>]
    let tryLast (array: 'T MemArray) =
        if array.Length = 0 then None 
        else Some array.[array.Length-1]

    [<CompiledName("Initialize")>]
    let inline init (count : int) initializer = 
        if count < 0 then invalidArg "count" "Must be positive" else
        let ma = new MemArray<_>(count)
        for i = 0 to count - 1 do 
            ma.[i] <- initializer i
        ma

    [<CompiledName("ZeroCreate")>]
    let inline zeroCreate count = init count (fun _ -> LanguagePrimitives.GenericZero)

    [<CompiledName("Create")>]
    let create (count: int) (value: 'T) =
        if count < 0 then invalidArg "count" "Must be positive" else
        let array: 'T MemArray = new MemArray<_>(count)
        for i = 0 to Operators.Checked.(-) array.Length 1 do // use checked arithmetic here to satisfy FxCop
            array.[i] <- value
        array

    [<CompiledName("TryHead")>]
    let tryHead (array: 'T MemArray) =
        if array.Length = 0 then None
        else Some array.[0]

    [<CompiledName("IsEmpty")>]
    let isEmpty (array: 'T MemArray) = 
        array.Length = 0

    [<CompiledName("Tail")>]
    let tail (array: 'T MemArray) =
        if array.Length = 0 then invalidArg "array" ("MemArray is empty")            
        array.[array.Length - 1]

    [<CompiledName("Empty")>]
    let empty<'T when 'T : unmanaged> : 'T  MemArray = new MemArray<_>(null, 0L, false, None)

*)
