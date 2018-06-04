explicit recursion - not a good idea, code repeat, compiler not helping much

fold - destroy data structure bottom-up
unfold - build structure top-down
refold - unfold / fold

For recursion schemes you need:
  - Functor
  - Fix-point type (most of the type)
  - Algebra and/or coalgebra
  
  
Binary tree example
 - add type parameter & make it a functor
 
```scala
implicit val treeFunctor = new Functor[TreeF] {
 def map[A, B](fa: TreeF[A])(f: A != B): TreeF[B] = fa match {
 case Node(l, r) != Node(f(l), f(r))
 case Leaf(i) != Leaf[B](i)
 }
}
```
 
 - different shapes of tree will have different types
 - introduce Fix type (Fix[TreeF])
 - (co)algebras - what to do with a single layer
     - algebras - collapse, 1 layer at a time
        - F[A] => A
     - coalgebras, build-up
        - A => F[A]
     - A is referred to (co)algebra's carrier
     - A is not a type inside the tree, but a type of a result
     
Hylomorphisms
 - pattern-functor | |
 - algebra |x| => x
 - coalgebra x => |x|
 
```scala
def hylo[F[_], A, B](a:A)(alg: Algebra[F, B], coalg: Coalgebra[F, A]): B =
  alg(coalg(a) map (hylo(_)(alg, coalg)))
```
  
hylo considers just one layer at the time, it never builds a full tree 

