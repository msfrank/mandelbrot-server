/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.core.http

import akka.actor.{Props, Actor, ActorLogging}
import akka.io.IO
import akka.util.Timeout
import spray.io.{ServerSSLEngineProvider, PipelineContext}
import javax.net.ssl.{SSLEngine, TrustManagerFactory, KeyManagerFactory, SSLContext}
import java.security.KeyStore
import java.io.FileInputStream

import io.mandelbrot.core.notification.NotificationService
import io.mandelbrot.core.registry.RegistryService
import io.mandelbrot.core.state.StateService
import io.mandelbrot.core.message.MessageStream
import io.mandelbrot.core.history.HistoryService
import io.mandelbrot.core.ServerConfig

/**
 * HttpServer is responsible for listening on the HTTP port, accepting connections,
 * and handing them over to the ApiService for processing.
 */
class HttpServer(val settings: HttpSettings) extends Actor with ApiService with ActorLogging {
  import spray.can.Http

  implicit val system = context.system
  implicit val dispatcher = context.dispatcher
  val actorRefFactory = context
  val registryService = RegistryService(system)
  val stateService = StateService(system)
  val historyService = HistoryService(system)
  val notificationService = NotificationService(system)
  val messageStream = MessageStream(system)

  // config
  val timeout: Timeout = settings.requestTimeout

  // if tls is enabled, then create an SSLContext
  val sslContext: Option[SSLContext] = settings.tls match {
    case Some(tlsSettings) =>
      val keystore = KeyStore.getInstance("JKS")
      keystore.load(new FileInputStream(tlsSettings.keystore), tlsSettings.keystorePassword.toCharArray)
      val truststore = KeyStore.getInstance("JKS")
      truststore.load(new FileInputStream(tlsSettings.truststore), tlsSettings.truststorePassword.toCharArray)
      val keymanagerFactory = KeyManagerFactory.getInstance("SunX509")
      keymanagerFactory.init(keystore, tlsSettings.keymanagerPassword.toCharArray)
      val trustmanagerFactory = TrustManagerFactory.getInstance("SunX509")
      trustmanagerFactory.init(truststore)
      val sslContext = SSLContext.getInstance("TLS")
      sslContext.init(keymanagerFactory.getKeyManagers, trustmanagerFactory.getTrustManagers, null)
      Some(sslContext)
    case None =>
      None
  }

  /* return the configured SSLContext if specified */
  implicit val sslContextProvider: (PipelineContext => Option[SSLContext]) = (ctx) => sslContext

  /* set SSLEngine options if SSLContext is specified */
  implicit val sslEngineProvider: (PipelineContext => Option[SSLEngine]) = (ctx) => sslContext match {
      case Some(ssl) =>
        val sslEngine = ssl.createSSLEngine()
        sslEngine.setUseClientMode(false)
        settings.tls.get.clientAuth match {
          case TlsClientAuthRequired => sslEngine.setNeedClientAuth(true)
          case TlsClientAuthRequested => sslEngine.setWantClientAuth(true)
          case otherwise =>
        }
        Some(sslEngine)
      case None => None
  }


  override def preStart() {
    IO(Http) ! Http.Bind(self, settings.interface, port = settings.port, backlog = settings.backlog)
    log.debug("binding to %s:%d with backlog %d%s".format(settings.interface, settings.port, settings.backlog,
      if (settings.tls.isDefined) "and TLS enabled" else ""
    ))
  }

  def receive = runRoute(routes) orElse {
    case bound: Http.Bound => log.debug("bound HTTP listener to {}", bound.localAddress)
  }
}

object HttpServer {
  def props(settings: HttpSettings) = Props(classOf[HttpServer], settings)
}

