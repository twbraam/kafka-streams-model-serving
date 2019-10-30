package org.twbraam.modelServer.model.PMML

/* Created by boris on 5/9/17. */

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.dmg.pmml.{FieldName, PMML}
import org.jpmml.evaluator._
import org.jpmml.model.{PMMLUtil, VisitorBattery}
import org.jpmml.model.visitors.{AttributeInternerBattery, AttributeOptimizerBattery, ListFinalizerBattery}
import org.jpmml.evaluator.visitors.{ElementInternerBattery, ElementOptimizerBattery}

import org.twbraam.model.modeldescriptor.ModelDescriptor
import org.twbraam.model.winerecord.WineRecord
import org.twbraam.modelServer.model.{Model, ModelFactory, ModelToServe}

import scala.collection.JavaConverters._
import scala.collection._

/**
 * Handle model exchange from remote training system using PMML and implement scoring with it.
 */
class PMMLModel(inputStream: Array[Byte]) extends Model {

  var arguments: mutable.Map[FieldName, FieldValue] = mutable.Map[FieldName, FieldValue]()

  // Marshall PMML
  val pmml: PMML = PMMLUtil.unmarshal(new ByteArrayInputStream(inputStream))

  // Optimize model // Optimize model
  PMMLModel.optimize(pmml)

  // Create and verify evaluator
  val evaluator: ModelEvaluator[_] = ModelEvaluatorFactory.newInstance.newModelEvaluator(pmml)
  evaluator.verify()

  // Get input/target fields
  val inputFields: List[InputField] = evaluator.getInputFields.asScala.toList
  val target: TargetField = evaluator.getTargetFields.get(0)
  val tname: FieldName = target.getName

  override def score(record: WineRecord): Option[Double] = {

    val arguments: Map[FieldName, FieldValue] = inputFields.map { field => (
      field.getName,
      field.prepare(getValueByName(record, field.getName.getValue))
    )}.toMap

    // Calculate Output// Calculate Output
    val result = evaluator.evaluate(arguments.asJava)

    // Prepare output
    result.get(tname) match {
      case c: Computable => Some(c.getResult.toString.toDouble)
      case score: Double => Some(score)
      case score: Int => Some(score.toDouble)
      case x => println(x); None
    }
  }

  override def cleanup(): Unit = {}

  private def getValueByName(input: WineRecord, name: String): Double =
    PMMLModel.names.get(name) match {
      case Some(index) =>
        val v = input.getFieldByNumber(index + 1)
        v.asInstanceOf[Double]
      case _ => .0
    }

  override def toBytes: Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    PMMLUtil.marshal(pmml, stream)
    stream.toByteArray
  }

  override def getType: Long = ModelDescriptor.ModelType.PMML.value
}

object PMMLModel extends ModelFactory {
  val optimizers = new VisitorBattery

  // Pre-parsing PMML elements
  optimizers.addAll(new AttributeOptimizerBattery)
  optimizers.addAll(new ElementOptimizerBattery)

  // Getting rid of duplicate PMML attribute values and PMML elements
  optimizers.addAll(new AttributeInternerBattery)
  optimizers.addAll(new ElementInternerBattery)

  // Freezing the final representation of PMML elements
  optimizers.addAll(new ListFinalizerBattery)

  def optimize(pmml: PMML): Unit = this.synchronized {
      try {
        optimizers.applyTo(pmml)
      } catch {
        case t: Throwable =>
          println(s"Error optimizing model for optimizer $optimizers")
          t.printStackTrace()
          throw t
      }
  }

  private val names = Map(
    "fixed acidity" -> 0,
    "volatile acidity" -> 1, "citric acid" -> 2, "residual sugar" -> 3,
    "chlorides" -> 4, "free sulfur dioxide" -> 5, "total sulfur dioxide" -> 6,
    "density" -> 7, "pH" -> 8, "sulphates" -> 9, "alcohol" -> 10
  )

  // Exercise:
  // The previous definition of `names` hard codes data about the records being scored.
  // Make this class more abstract and reusable. There are several possible ways:
  // 1. Make this class an abstract class and subclass a specific kind for wine records.
  // 2. Keep this class concrete, but use function arguments to provide the `data` array. (Better)

  override def create(input: ModelToServe): Model = {
    new PMMLModel(input.model)
  }

  override def restore(bytes: Array[Byte]): Model = new PMMLModel(bytes)
}
