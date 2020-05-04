package com.github.mjakubowski84.parquet4s

import shapeless.Witness

trait Cursor {

  def path: Cursor.Path

  def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor]

  def accept[T, R](obj: T, visitor: Cursor.Visitor[T, R]): R

  def objective: String

}

object Cursor {

  type Path = Seq[String]
  object Path {
    val empty: Path = List.empty
    def apply(str: String): Path = str.split("\\.").toList
    def toString(path: Path): String = path.mkString(".")
  }

  def skipping(toSkip: Iterable[String]): Cursor = SkippingCursor(toSkip.map(Path.apply).toSet)

  def following(path: String): Cursor = FollowingCursor(Path(path))


  final class Completed(val path: Path, val objective: String) extends Cursor {
    override def accept[T, R](obj: T, visitor: Visitor[T, R]): R = visitor.onCompleted(this, obj)
    override def advance[FieldName <: Symbol : Witness.Aux]: Option[Cursor] = None
  }

  trait Active {
    this: Cursor =>
    override def accept[T, R](obj: T, visitor: Visitor[T, R]): R = visitor.onActive(this, obj)
  }

  trait Visitor[T, R] {
    def onCompleted(cursor: Cursor, obj: T): R
    def onActive(cursor: Cursor, obj: T): R
  }

}

private object SkippingCursor {
  def apply(toSkip: Set[Cursor.Path]): SkippingCursor = new SkippingCursor(Cursor.Path.empty, toSkip)
}

private class SkippingCursor private (val path: Cursor.Path, toSkip: Set[Cursor.Path]) extends Cursor with Cursor.Active {

  override lazy val objective: String = toSkip.map(Cursor.Path.toString).mkString("[", ", ", "]")

  override def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor] = {
    val newPath = path :+ implicitly[Witness.Aux[FieldName]].value.name
    if (toSkip.contains(newPath)) None
    else Some(new SkippingCursor(newPath, toSkip))
  }

}

private object FollowingCursor {
  def apply(toFollow: Cursor.Path): Cursor =
    if (toFollow.isEmpty) new Cursor.Completed(Cursor.Path.empty, "")
    else new FollowingCursor(Cursor.Path.empty, toFollow)
}

private class FollowingCursor private(val path: Cursor.Path, toFollow: Cursor.Path) extends Cursor with Cursor.Active {

  override lazy val objective: String = Cursor.Path.toString(toFollow)

  override def advance[FieldName <: Symbol: Witness.Aux]: Option[Cursor] = {
    val newPath = path :+ implicitly[Witness.Aux[FieldName]].value.name
    if (toFollow == newPath)
      Some(new Cursor.Completed(newPath, objective))
    else if (toFollow.startsWith(newPath))
      Some(new FollowingCursor(newPath, toFollow))
    else None
  }

}
