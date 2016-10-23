package com.onzo.dynamodb.codec

import cats.laws._
import com.onzo.dynamodb.{Encoder, Decoder}

// Classes needed for tests but that are not tests by themselves would be better located in another package.
trait CodecLaws[A] {
  def decode: Decoder[A]

  def encode: Encoder[A]

  val name = "name"

  def codecRoundTrip(a: A): IsEq[A] =
    decode(name, encode(name, a)) <-> a
}

object CodecLaws {
  def apply[A](implicit d: Decoder[A], e: Encoder[A]): CodecLaws[A] = new CodecLaws[A] {
    val decode: Decoder[A] = d
    val encode: Encoder[A] = e
  }
}
