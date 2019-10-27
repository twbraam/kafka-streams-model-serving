package org.twbraam.modelServer.model

case class ServingResult(processed: Boolean, result: Double = .0, duration: Long = 0l)

object ServingResult {
  val noModel = ServingResult(processed = false)

  def apply(result: Double, duration: Long): ServingResult = ServingResult(processed = true, result, duration)
}