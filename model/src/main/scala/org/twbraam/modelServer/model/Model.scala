package org.twbraam.modelServer.model

import org.twbraam.model.winerecord.WineRecord


/**
 * Basic trait for a model. For simplicity, we assume the data to be scored are WineRecords.
 * Created by boris on 5/9/17.
 */
trait Model {
  def score(record: WineRecord): Option[Double]

  def cleanup(): Unit

  def toBytes: Array[Byte]

  def getType: Long
}
