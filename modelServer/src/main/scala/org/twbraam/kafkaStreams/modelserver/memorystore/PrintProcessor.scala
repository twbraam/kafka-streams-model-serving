package org.twbraam.kafkaStreams.modelserver.memorystore

import org.apache.kafka.streams.processor.AbstractProcessor
import org.twbraam.modelServer.model.ServingResult

/**
 * Implements a topology processor that just prints out the results to stdout.
 */
class PrintProcessor extends AbstractProcessor[Array[Byte], ServingResult]{

  override def process (key: Array[Byte], value: ServingResult ): Unit = {
    value.processed match {
      case true => println(s"Calculated quality - ${value.result} calculated in ${value.duration} ms")
      case _ => println("No model available - skipping")
    }
  }
}
