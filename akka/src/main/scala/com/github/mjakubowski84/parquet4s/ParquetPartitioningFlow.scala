package com.github.mjakubowski84.parquet4s


import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetWriter => HadoopParquetWriter}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration

object ParquetPartitioningFlow {

  type Partitioner[T] = T => (String, String)


  val DefaultMaxCount: Long = HadoopParquetWriter.DEFAULT_BLOCK_SIZE
  val DefaultMaxDuration: FiniteDuration = FiniteDuration(1, TimeUnit.MINUTES)


  def builder[T: ParquetRecordEncoder : ParquetSchemaResolver](basePath: String): Builder[T] =
    BuilderImpl(basePath)

  trait Builder[T] {
    def withMaxCount(maxCount: Long): Builder[T]
    def withMaxDuration(maxDuration: FiniteDuration): Builder[T]
    def withWriteOptions(writeOptions: ParquetWriter.Options): Builder[T]
    def withPartitionBy(partitionBy: Seq[T => String]): Builder[T]
    def addPartitionBy(partitionBy: T => String): Builder[T] // take string, builder should resolve it into skips + partitioner
    def build(): GraphStage[FlowShape[T, T]]
  }

  private case class BuilderImpl[T: ParquetRecordEncoder : ParquetSchemaResolver](
                        basePath: String,
                                                                                 // TODO max count per partition + max count at all
                        maxCount: Long = DefaultMaxCount,
                        maxDuration: FiniteDuration = DefaultMaxDuration,
                        writeOptions: ParquetWriter.Options = ParquetWriter.Options(),
                        partitionBy: Seq[T => String] = Seq.empty
                      ) extends Builder[T] {

    def withMaxCount(maxCount: Long): Builder[T] = copy(maxCount = maxCount)
    def withMaxDuration(maxDuration: FiniteDuration): Builder[T] = copy(maxDuration = maxDuration)
    def withWriteOptions(writeOptions: ParquetWriter.Options): Builder[T] = copy(writeOptions = writeOptions)
    def withPartitionBy(partitionBy: Seq[T => String]): Builder[T] = copy(partitionBy = partitionBy)
    def addPartitionBy(partitionBy: T => String): Builder[T] = copy(partitionBy = this.partitionBy :+ partitionBy)
    def build(): GraphStage[FlowShape[T, T]] = new ParquetPartitioningFlow(basePath, maxCount, maxDuration, partitionBy, writeOptions)
  }

}


// TODO basePath and so pn should be hadoop's one
private class ParquetPartitioningFlow[T: SkippingParquetRecordEncoder : SkippingParquetSchemaResolver](
                                                                                        basePath: String,
                                                                                        maxCount: Long,
                                                                                        maxDuration: FiniteDuration,
                                                                                        partitionBy: Seq[T => String],
                                                                                        writeOptions: ParquetWriter.Options
                                                                                      ) extends GraphStage[FlowShape[T, T]] {
  val in: Inlet[T] = Inlet[T]("ParquetPartitioningFlow.in")
  val out: Outlet[T] = Outlet[T]("ParquetPartitioningFlow.out")
  val shape: FlowShape[T, T] = FlowShape.of(in, out)
  private val logger = LoggerFactory.getLogger("ParquetPartitioningFlow")
  private val schema = SkippingParquetSchemaResolver.resolveSchema[T](List.empty) // TODO resolve in builder
  private val vcc = writeOptions.toValueCodecConfiguration
  private def encode(t: T): RowParquetRecord = ParquetRecordEncoder.encode[T](t, vcc)

  private class FlowWithPassthroughLogic extends TimerGraphStageLogic(shape) with InHandler with OutHandler {
    private var rotationCount = -1L
    private var writers: scala.collection.immutable.Map[String, ParquetWriter.InternalWriter] = Map.empty
    private val timerKey = "ParquetPartitioningFlow.Rotation" // TODO constant
    private var shouldRotate = true
    private var count = 0L

    setHandlers(in, out, this)

    private def partitionPath(msg: T): String = partitionBy.foldLeft(basePath)(_ + "/" + _.apply(msg))

    // TODO add compression codec to default path name
    private def newFileName: String = UUID.randomUUID().toString + ".parquet"

    private def write(msg: T): Unit = {
      val writerPath = partitionPath(msg)
      writers.get(writerPath) match {
        case Some(writer) =>
          writer.write(encode(msg))
        case None =>
          val writer = ParquetWriter.internalWriter(
            new Path(writerPath, newFileName),
            schema = schema,
            options = writeOptions
          )
          writers = writers.updated(writerPath, writer)
          writer.write(encode(msg))
      }
    }

    private def close(): Unit = {
      writers.valuesIterator.foreach(_.close())
      writers = Map.empty
    }

    override def preStart(): Unit =
      schedulePeriodically(timerKey, maxDuration)

    override def onTimer(timerKey: Any): Unit =
      if (this.timerKey == timerKey) {
        shouldRotate = true
      }

    override def onPush(): Unit = {
      if (shouldRotate) {
        close()
        rotationCount += 1 // TODO only for debugging / testing? should be removed
        shouldRotate = false
        count = 0
        logger.debug("Rotation number " + rotationCount)
      }
      val msg = grab(in)
      write(msg)
      count += 1

      if (count >= maxCount) {
        shouldRotate = true
      }

      push(out, msg)
    }

    override def onPull(): Unit =
      if (!isClosed(in) && !hasBeenPulled(in)) {
        pull(in)
      }

    override def onUpstreamFinish(): Unit = {
      close()
      completeStage()
    }

  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new FlowWithPassthroughLogic()
}