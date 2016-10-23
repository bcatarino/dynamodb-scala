package com.onzo.dynamodb

import java.util.UUID
import cats.functor.Contravariant
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import scala.collection.generic.IsTraversableOnce

trait Encoder[A] {
  //self =>
  // Rename apply to encode or something else, as apply is easily confused with the typical companion object apply method
  def apply(a: A): AttributeValue

  // Rename apply to encode or something else, as apply is easily confused with the typical companion object apply method
  def apply(name: String, a: A): Map[String, AttributeValue] = {
    Map(name -> apply(a))
  }

  def contramap[B](f: B => A): Encoder[B] = Encoder.instance(b => apply(f(b)))
}

object Encoder {
  // I don't understand why this method exists. The whole point of an apply method is creating a new instance. If we are
  // returning an already existing instance, this method is not needed and can be deleted.
  def apply[A](implicit e: Encoder[A]): Encoder[A] = e

  // It would be good to have parameter names more descriptive than 'f' or 'a'. This is generic to the code in general,
  // not only this particular method
  def instance[A](f: A => AttributeValue): Encoder[A] = new Encoder[A] {
    def apply(a: A): AttributeValue = f(a)
  }

  implicit def encodeTraversableOnce[A0, C[_]](implicit
                                               e: Encoder[A0],
                                               is: IsTraversableOnce[C[A0]] {type A = A0}
                                              ): Encoder[C[A0]] =
    instance { list =>
      // Number of lines could be slightly reduced by mapping the original list to a scala collection and convert to
      // a java collection just right at the end.
      val items = new java.util.ArrayList[AttributeValue]()

      is.conversion(list).foreach { a =>
        // Variables a and functions e make the code very unreadable. I need to scan the code to know e is the encoder.
        items add e(a)
      }

      new AttributeValue().withL(items)
    }

  implicit val encodeAttributeValue: Encoder[AttributeValue] = instance(identity)
  implicit val encodeString: Encoder[String] = instance(new AttributeValue().withS(_))
  implicit val encodeBoolean: Encoder[Boolean] = instance(new AttributeValue().withBOOL(_))
  implicit val encodeFloat: Encoder[Float] = instance(a => new AttributeValue().withN(a.toString))
  implicit val encodeDouble: Encoder[Double] = instance(a => new AttributeValue().withN(a.toString))
  implicit val encodeByte: Encoder[Byte] = instance(a => new AttributeValue().withN(a.toString))
  implicit val encodeShort: Encoder[Short] = instance(a => new AttributeValue().withN(a.toString))
  implicit val encodeInt: Encoder[Int] = instance(a => new AttributeValue().withN(a.toString))
  implicit val encodeLong: Encoder[Long] = instance(a => new AttributeValue().withN(a.toString))
  implicit val encodeBigInt: Encoder[BigInt] = instance(a => new AttributeValue().withN(a.toString))
  implicit val encodeBigDecimal: Encoder[BigDecimal] = instance(a => new AttributeValue().withN(a.toString))
  implicit val encodeUUID: Encoder[UUID] = instance(uuid => new AttributeValue().withS(uuid.toString))

  // Could be a good idea to move the encoders (especially the custom ones) to another object.
  implicit def encodeOption[A](implicit e: Encoder[A]): Encoder[Option[A]] = new Encoder[Option[A]] {
    //self =>
    override def apply(a: Option[A]): AttributeValue = e(a.get)

    override def apply(name: String, a: Option[A]): Map[String, AttributeValue] = {
      // Feels strange that it's checking if an option is defined and then add the option anyway. Shouldn't it add the
      // value inside the option, when it's defined?
      if(a.isDefined)
        Map(name -> apply(a))
      else
        Map.empty[String,AttributeValue]
    }
  }


  implicit def encodeMapLike[M[K, +V] <: Map[K, V], V](implicit
                                                       e: Encoder[V]
                                                      ): Encoder[M[String, V]] = Encoder.instance { m =>
    val map = m.map {
      case (k, v) => (k, e(v))
    }
    import scala.collection.JavaConversions._

    new AttributeValue().withM(map)
  }

  def encodeEither[A, B](leftKey: String, rightKey: String)(implicit
                                                            ea: Encoder[A],
                                                            eb: Encoder[B]
  ): Encoder[Either[A, B]] = instance { a =>
    val map = new java.util.HashMap[String, AttributeValue]()
    a.fold(
      a => map.put(leftKey, ea(a)),
      b => map.put(rightKey, eb(b)))
    new AttributeValue().withM(map)
  }

  implicit val contravariantEncode: Contravariant[Encoder] = new Contravariant[Encoder] {
    def contramap[A, B](e: Encoder[A])(f: B => A): Encoder[B] = e.contramap(f)
  }

}
