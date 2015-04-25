package io.mandelbrot.core.http.json

import spray.json.DefaultJsonProtocol

object JsonProtocol extends DefaultJsonProtocol
with ConstantsProtocol
with StandardProtocol
with EntityProtocol
with MetricsProtocol
with NotificationProtocol
with RegistryProtocol
with StateProtocol
with SystemProtocol
