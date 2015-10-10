package io.mandelbrot.core.model.json

import java.nio.charset.Charset

import spray.http.{ContentTypes, HttpEntity}
import spray.json.JsValue

/**
 *
 */
object JsonBody {
  val charset = Charset.defaultCharset()
  def apply(js: JsValue): HttpEntity = {
    HttpEntity(ContentTypes.`application/json`, js.prettyPrint.getBytes(charset))
  }
}
