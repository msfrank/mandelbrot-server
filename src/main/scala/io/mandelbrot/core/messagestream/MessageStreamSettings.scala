package io.mandelbrot.core.messagestream

import com.typesafe.config.Config
import scala.collection.JavaConversions._

class MessageStreamSettings(val endpoint: String, val subscriptions: Option[Vector[String]])

object MessageStreamSettings {
  def parse(config: Config): MessageStreamSettings = {
    val endpoint = config.getString("endpoint")
    val subscriptions = if (!config.hasPath("subscriptions")) None else Some(config.getStringList("subscriptions").toVector)
    new MessageStreamSettings(endpoint, subscriptions)
  }
}
