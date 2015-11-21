package io.mandelbrot.core.model.json

import spray.json.DefaultJsonProtocol

object JsonProtocol extends DefaultJsonProtocol
with ConstantsProtocol
with StandardProtocol
with EntityProtocol
with IngestProtocol
with MetricsProtocol
with NotificationProtocol
with RegistryProtocol
with StateProtocol
with CheckProtocol
