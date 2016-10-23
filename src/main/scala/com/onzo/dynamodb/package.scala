package com.onzo

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import shapeless.LUBConstraint._
import shapeless._
import shapeless.ops.hlist._


package object dynamodb {

  // Not sure where the symbol λ comes from or what it's doing, but guessing this can become an encoding error (doesn't
  // compile for me, at least). Can this be replaced with anything else more standard?
  implicit class KeysHList[
  A <: HList : <<:[KeyLike[_]]#λ,
  M <: HList,
  N <: HList,
  T <: HList,
  Primary
  ](a: A) {

    // still needed or should to do be removed as well as the code it refers to?
    // todo remove?
    val optionalRangeKey = RangeKey[Int]("rangeKeyCheat")

    def as[B](implicit entityGen: Generic.Aux[B, M]
              , zipper: Zip.Aux[A :: M :: HNil, N]
              , collectPrimaryKey: CollectFirst.Aux[A, HlistHelper.findPrimaryKey.type, PrimaryKey[Primary]]
              , collectFirst2: CollectFirst.Aux[N, HlistHelper.findPrimaryKeyValue.type, Primary]
              , foldLeftEncode: LeftFolder.Aux[N, Map[String, AttributeValue], HlistHelper.EncodeHlist.type, Map[String, AttributeValue]]
              , zipperMap: ZipConst.Aux[HlistHelper.DecodeHlist.R, A, T]
              , decodeMapper: Mapper.Aux[HlistHelper.DecodeHlist.type, T, M]
              , findAllKeyName: LeftFolder.Aux[A, List[String], HlistHelper.findAllKeyName.type, List[String]]
             ): TableMapper[B] = {

      val _primaryKey: PrimaryKey[Primary] = a.collectFirst(HlistHelper.findPrimaryKey)(collectPrimaryKey)
      val _rangeKey = a.runtimeList.collectFirst({
        case r: RangeKey[_] => r
      })

      val names = a.foldLeft(List.empty[String])(HlistHelper.findAllKeyName)(findAllKeyName)

      // Again, variable and function names are too generic to make the code easy to read.
      // Could see this as a non-anonymous class, in order to split code a bit more.
      new TableMapper[B] {
        override val primaryKey: Option[(String, Encoder[B])] = {
          Some(_primaryKey.name -> Encoder.instance {
            b: B =>
              val zipped = a.zip(entityGen.to(b))(zipper)

              val v = zipped.collectFirst(HlistHelper.findPrimaryKeyValue)(collectFirst2)
              _primaryKey.encoder.apply(v)
          })
        }

        override def rangeKey: Option[(String, Encoder[B])] = {
          _rangeKey.map { key =>
            key.name -> Encoder.instance {
              b: B =>
                val zipped = a.zip(entityGen.to(b))(zipper)

                // at this point we know there is a RangeKey and we know that entityGen exist
                zipped.runtimeList.collectFirst({
                  case (r: RangeKey[A], a: A) => r.encoder.apply(a)
                }).get
            }
          }
        }

        override def encode(b: B): Map[String, AttributeValue] = {
          val zipped = a.zip(entityGen.to(b))
          zipped.foldLeft(Map.empty[String, AttributeValue])(HlistHelper.EncodeHlist)(foldLeftEncode)
        }

        override def decode(items: Map[String, AttributeValue]): B = {
          val zipped = a.zipConst((items, names))(zipperMap)
          val hlist = zipped.map(HlistHelper.DecodeHlist)(decodeMapper)
          entityGen.from(hlist)
        }
      }
    }
  }

}
