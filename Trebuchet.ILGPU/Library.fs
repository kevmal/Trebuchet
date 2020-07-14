namespace Trebuchet.ILGPU
#nowarn "9"
open ILGPU

type Crap() = 
    static member MyKernel(index : Index1, r : ArrayView<float32>, a : ArrayView<float32>, b : ArrayView<float32>) = 
        r.[index] <- a.[index] + b.[index]
module Say =
    open ILGPU.Runtime.CPU
    let minfo = typeof<Crap>.GetMethod("MyKernel")

    let add (r : float32 []) (a : float32 []) (b : float32 []) =    
        use ctx = new Context()
        use acc = new CPUAccelerator(ctx)
        let k = acc.LoadAutoGroupedKernel(minfo)
        use ap = fixed a
        use bp = fixed b
        use rp = fixed r
        let av = ArrayView<float32>(ILGPU.Runtime.ViewPointerWrapper.Create(ap), Index1 0, Index1(a.Length))
        let bv = ArrayView<float32>(ILGPU.Runtime.ViewPointerWrapper.Create(bp), Index1 0, Index1(b.Length))
        let rv = ArrayView<float32>(ILGPU.Runtime.ViewPointerWrapper.Create(rp), Index1 0, Index1(r.Length))
        k.Launch(acc.DefaultStream,Index1(a.Length),rv,av,bv)
        acc.Synchronize()
        r
        