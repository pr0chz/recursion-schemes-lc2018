package lc2018

import org.apache.avro.{LogicalTypes, _}
import matryoshka._, implicits._, patterns.EnvT
import scala.collection.immutable.ListMap
import scalaz._, Scalaz._

import scala.language.higherKinds
import scala.collection.JavaConverters._

/**
  * There is a problem that makes writing SchemaF <-> Avro (co)algebras more difficult.
  *
  * As a matter of fact Avro mandates that, when building a Schema, all records (the Avro
  * equivalent to our StructF) are registered using a unique name.
  *
  * This is problematic to our algebra-based method because with the algebras we've seen so
  * far we only care about one "layer" at a time, so there is no way to know the names we've
  * already used for ther records we've registered so far.
  *
  * Fortunately, we have at least two solutions to that problem. But before going any further,
  * maybe you can take a few minutes to try and imagine how we can solve that problem in general,
  * even if you don't know how to implement your solution using recursion-schemes yet.
  */
trait SchemaToAvroAlgebras extends Labelling with UsingARegistry with AvroCoalgebra {}

/**
  * The first solution comes from the observation that our schemas are in fact trees. And trees have
  * this nice property that each node have a unique path that goes from the root to it. If we can use
  * that unique path as the names of our records, we're good to go. So this solution boils down to
  * labelling each "node" of a schema with its path, and then use that path to form the names we
  * use to register our records.
  */
trait Labelling {

  /**
    * So lets define out Path as being simply a list of strings. These strings will be the field names
    * we need to traverse from the root to get to a specific element of our schema.
    */
  type Path = List[String]

  /**
    * Here is the "special trick" of the current solution.
    *
    * EnvT is a kind of "glorified pair". Given a label type E and a (pattern)-functor F, it allows us
    * to label each "node" of a T[F] with a value of type E while retaining the original structure. In
    * other words, if F is a functor, then EnvT[E, F, ?] is a functor as well.
    */
  type Labelled[A] = EnvT[Path, SchemaF, A]

  /**
    * If we are to label each "node" of a schema with its own path, we obviously need to go from the root
    * down to the leaves, so we definitely want to write a coalgebra.
    * This one might look a bit scarry though, but fear not, it's not as complcated as it looks. Lets just
    * follow the types together.
    *
    * A Coalgebra[F, A] is just a function A => F[A]. So the coalgebra bellow is just a function
    *  (Path, T[SchemaF]) => Labelled[(Path, T[SchemaF])
    * Expanding the Labelled alias it becomes
    *  (Path, T[SchemaF]) => EnvT[Path, SchemaF, (Path, T[SchemaF])]
    *
    * Ok, maybe it still looks a bit scarry...
    *
    * Lets try to put it differently. Assume you will be given a "seed" consisting of a whole schema and an
    * initial path (that will start empty). Your job is to use that to produce an EnvT that will contain
    * the path of the node you just saw (the "root" of the schema that was in the seed), and the node itself
    * but modified such that its "content" is not just a "smaller schema" as it was initially, but a new "seed"
    * consisting of a (larger) path, and the said "smaller schema".
    */
  def labelNodesWithPath[T](implicit T: Recursive.Aux[T, SchemaF]): Coalgebra[Labelled, (Path, T)] = {
    // A => F[A]

    case (path, t) =>
      t.project match {
        case StructF(fields) => EnvT((path, StructF(fields.map(x => (x._1, (x._1 :: path, x._2))))))
        case x               => EnvT((path, x.map((path, _))))
      }
  }

  /**
    * Now the algebra (that we had no way to write before) becomes trivial. All we have to do is to use
    * the path labelling each "node" as the name we need when registering a new avro record.
    *
    * To extract the label (resp. node) of an EnvT you can use pattern-matching (EnvT contains only a pair
    * (label, node)), or you can use the `ask` and `lower` methods that return the label and node respectively.
    */
  def labelledToSchema: Algebra[Labelled, Schema] =
    (x: Labelled[Schema]) => {
      x.lower match {
        case StructF(fields) =>
          val fs = SchemaBuilder.record(x.ask.mkString(".")).fields
          for ((name, t) <- fields) {
            fs.name("name").`type`(t).noDefault()
          }
          fs.endRecord()

        case ArrayF(element) => SchemaBuilder.array().items(element)
        case BooleanF()      => Schema.create(Schema.Type.BOOLEAN)
        case DateF()         => LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
        case DoubleF()       => Schema.create(Schema.Type.DOUBLE)
        case FloatF()        => Schema.create(Schema.Type.FLOAT)
        case IntegerF()      => Schema.create(Schema.Type.INT)
        case LongF()         => Schema.create(Schema.Type.LONG)
        case StringF()       => Schema.create(Schema.Type.STRING)
      }
    }

  def schemaFToAvro[T](schemaF: T)(implicit T: Recursive.Aux[T, SchemaF]): Schema =
    (List.empty[String], schemaF).hylo(labelledToSchema, labelNodesWithPath)
}

/**
  * That first solution was (relatively) simple but it is not completely satisfying.
  * We needed both an algebra and a coalgebra to got from our SchemaF to Avro's Schema, which forced us to
  * use hylo.
  *
  * Fortunately, every scheme (and the related algebra) come with a "monadic" version. In this version, we
  * have to "wrap" the result of our algebras inside our monad of choice. The scheme will then use this
  * monad's bind at each step. That has plenty of cool uses.
  *
  * We can for example "short-circuit" the traversal by using \/ or Option as our monad. Or in this very case
  * we can use the State monad to keep track of what records we've already created.
  *
  * A note though: in order to use monadic schemes, we need a Traverse instance for our pattern-functor.
  */
trait UsingARegistry {

  type Registry[A] = State[Map[Int, Schema], A]

  def fingerprint(fields: Map[String, Schema]): Int = fields.hashCode

  def useARegistry: AlgebraM[Registry, SchemaF, Schema] = {
    case StructF(fields) =>
      State { registry =>
        val fp = fingerprint(fields)
        if (registry.contains(fp)) (registry, registry(fp))
        else {
          val schema = SchemaBuilder.record(fp.toString).fields
          for ((name, t) <- fields) {
            schema.name("name").`type`(t).noDefault()
          }
          val sch = schema.endRecord()
          (registry + (fp -> sch), sch)
        }
      }

    case ArrayF(field) =>
      State.state(SchemaBuilder.array.items(field))

    case BooleanF() => State.state(Schema.create(Schema.Type.BOOLEAN))
    case DateF()    => State.state(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
    case DoubleF()  => State.state(Schema.create(Schema.Type.DOUBLE))
    case FloatF()   => State.state(Schema.create(Schema.Type.FLOAT))
    case IntegerF() => State.state(Schema.create(Schema.Type.INT))
    case LongF()    => State.state(Schema.create(Schema.Type.LONG))
    case StringF()  => State.state(Schema.create(Schema.Type.STRING))
  }

  implicit def schemaFTraverse: Traverse[SchemaF] = new Traverse[SchemaF] {
    override def traverseImpl[G[_], A, B](fa: SchemaF[A])(f: A => G[B])(implicit ev: Applicative[G]): G[SchemaF[B]] = {
      fa match {
        case StructF(fields) =>

          val x = fields.map { case (k, v) => (k, f(v)) }

        case ArrayF(element) =>
        case BooleanF() =>
        case DateF() =>
        case DoubleF() =>
        case FloatF() =>
        case IntegerF() =>
        case LongF() =>
        case StringF() =>
      }
    }
  }

  def toAvro[T](schemaF: T)(implicit T: Recursive.Aux[T, SchemaF]): Schema =
    schemaF.cataM(useARegistry).run(Map.empty)._2
}

trait AvroCoalgebra {

  /**
    * Of course we also need a coalgebra to go from Avro to SchemaF
    * Since there are some avro shcemas that we do not handle here,
    * we need a CoalgebraM, but we're not really interested in providing meaningful errors
    * here, so we can use Option as our monad.
    */
  def avroToSchemaF: CoalgebraM[Option, SchemaF, Schema] = TODO
}
