namespace Trebuchet.Algorithms


module Tree = 
    type Node = 
        | Leaf of weight:double
        | Tree of feature : int * split :double * left : Node * right : Node
