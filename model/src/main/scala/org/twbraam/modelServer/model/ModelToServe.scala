package org.twbraam.modelServer.model

import org.twbraam.model.modeldescriptor.ModelDescriptor

import scala.util.Try


/**
 * A wrapper for metadata about a model.
 * Created by boris on 5/8/17.
 */
case class ModelToServe(name: String, description: String,
                        modelType: ModelDescriptor.ModelType, model: Array[Byte], dataType: String) {}


object ModelToServe {
  def fromByteArray(message: Array[Byte]): Try[ModelToServe] = Try {
    val m: ModelDescriptor = ModelDescriptor.parseFrom(message)
    if (m.messageContent.isData) {
      new ModelToServe(m.name, m.description, m.modeltype, m.getData.toByteArray, m.dataType)
    } else {
      throw new Exception("Location based is not yet supported")
    }
  }
}

