package com.github.mjakubowski84.parquet4s

import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

private[parquet4s] trait StringLens[T] {

  def apply(cursor: Cursor, obj: T): Either[StringLens.LensError, String]

}

private[parquet4s] object StringLens {

  case class LensError(cursor: Cursor, message: String)

  implicit val hnilStringLens: StringLens[HNil] = new StringLens[HNil] {
    override def apply(cursor: Cursor, obj: HNil): Either[LensError, String] =
      Left(LensError(cursor, s"Field '${cursor.objective}' does not exist."))
  }

  implicit def headValueLens[FieldName <: Symbol, Head, Tail <: HList](implicit
                                                                       witness: Witness.Aux[FieldName],
                                                                       headVisitor: FieldVisitor[Head] = defaultFieldVisitor[Head],
                                                                       tailLens: StringLens[Tail]
  ): StringLens[FieldType[FieldName, Head] :: Tail] =
    new StringLens[FieldType[FieldName, Head] :: Tail] {
      override def apply(cursor: Cursor, field: FieldType[FieldName, Head] :: Tail): Either[LensError, String] = {
        cursor.advance[FieldName] match {
          case Some(newCursor) =>
            newCursor.accept(field.head, headVisitor)
          case None =>
            tailLens.apply(cursor, field.tail)
        }
      }
    }

  implicit def genericStringLens[T, R](implicit
                                       gen: LabelledGeneric.Aux[T, R],
                                       lens: Lazy[StringLens[R]]): StringLens[T] =
    new StringLens[T] {
      override def apply(cursor: Cursor, obj: T): Either[LensError, String] =
        lens.value.apply(cursor, gen.to(obj))
    }

  trait FieldVisitor[T] extends Cursor.Visitor[T, Either[LensError, String]]

  implicit def productFieldVisitor[T](implicit lens: StringLens[T]): FieldVisitor[T] = new FieldVisitor[T] {
    override def onCompleted(cursor: Cursor, field: T): Either[LensError, String] =
      Left(LensError(cursor, "Cannot partition by a Product class."))

    override def onActive(cursor: Cursor, field: T): Either[LensError, String] =
      lens.apply(cursor, field)
  }

  implicit val stringFieldVisitor: FieldVisitor[String] = new FieldVisitor[String] {
    override def onCompleted(cursor: Cursor, field: String): Either[LensError, String] =
      Right(field)

    override def onActive(cursor: Cursor, obj: String): Either[LensError, String] =
      Left(LensError(cursor, s"Attempted to access child field '${cursor.objective}' from parent String."))
  }

  private def defaultFieldVisitor[T]: FieldVisitor[T] = new FieldVisitor[T] {
    override def onCompleted(cursor: Cursor, field: T): Either[LensError, String] =
      Left(LensError(cursor, "Only String field can be used for partitioning."))

    override def onActive(cursor: Cursor, field: T): Either[LensError, String] =
      Left(LensError(cursor, "Only String field can be used for partitioning."))
  }

  def apply[T](obj: T, path: String)(implicit lens: StringLens[T]): String = {

    lens.apply(Cursor.following(path), obj) match {
      case Left(LensError(cursor, message)) =>
        val cursorPath = cursor.path.mkString(".")
        throw new IllegalArgumentException(s"Invalid element at path '$cursorPath'. $message")
      case Right(result) =>
        result
    }

  }

}
