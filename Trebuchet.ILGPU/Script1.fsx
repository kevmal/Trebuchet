#r @"D:\AppData\nuget\packages\ilgpu\0.8.1.1\lib\net47\ILGPU.dll"
#r @"D:\AppData\nuget\packages\ilgpu.algorithms\0.8.0\build\../lib/net47/Release/ILGPU.Algorithms.dll"


open ILGPU
open ILGPU.Runtime.Cuda
open ILGPU.Runtime.Cuda.API


let inline add (index : Index1, r : ArrayView< ^a>, a : ArrayView< ^a>, b : ArrayView< ^a>) =
    r.[index] <- a.[index] + b.[index]

type Crap() = 
    static member MyKernelFloat32(index : Index1, r : ArrayView<float32>, a : ArrayView<float32>, b : ArrayView<float32>) = 
        add(index,r,a,b)
    static member MyKernelInt32(index : Index1, r : ArrayView<int>, a : ArrayView<_>, b : ArrayView<_>) = 
        add(index,r,a,b)

let minfo = typeof<Crap>.GetMethod("MyKernelFloat32")
let a = [|1.f .. 1000.f|]
let b = [|1000.f .. -1.f .. 1.f|]
let r = Array.zeroCreate a.Length


do
    use ctx = new Context()
    use acc = new CudaAccelerator(ctx)
    let k = acc.LoadAutoGroupedKernel(minfo)
    let ab = acc.Allocate<float32> a.Length
    let bb = acc.Allocate<float32> b.Length
    let rb = acc.Allocate<float32> b.Length
    ab.CopyFrom(a,0,Index1 0,a.Length)
    bb.CopyFrom(b,0,Index1 0,b.Length)
    k.Launch(acc.DefaultStream,Index1(a.Length),rb.View,ab.View,bb.View)
    acc.Synchronize()
    rb.CopyTo(r,Index1 0,0,Index1 a.Length)
<@ $"""{(*comment*)""}""" @>
let r2 = 
    let minfo = typeof<Crap>.GetMethod("MyKernelInt32")
    let a = [|1 .. 1000|]
    let b = [|1000 .. -1 .. 1|]
    let r = Array.zeroCreate a.Length


    do
        use ctx = new Context()
        use acc = new CudaAccelerator(ctx)
        let cublas = ILGPU.Runtime.Cuda.CuBlas(acc)
        cublas.
        ArrayView
        cublas.
        let k = acc.LoadAutoGroupedKernel(minfo)
        let ab = acc.Allocate<int> a.Length
        let bb = acc.Allocate<int> b.Length
        let rb = acc.Allocate<int> b.Length
        printfn "%A" ab.CopyTo()
        ab.Cop
        ab.CopyFrom(a,0,Index1 0,a.Length)
        bb.CopyFrom(b,0,Index1 0,b.Length)
        k.Launch(acc.DefaultStream,Index1(a.Length),rb.View,ab.View,bb.View)
        acc.Synchronize()
        rb.CopyTo(r,Index1 0,0,Index1 a.Length)
    r




    