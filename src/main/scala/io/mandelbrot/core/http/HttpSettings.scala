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

import com.typesafe.config.Config
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

class HttpSettings(val interface: String,
                   val port: Int,
                   val backlog: Int,
                   val requestTimeout: FiniteDuration,
                   val tls: Option[TlsSettings])

object HttpSettings {
  def parse(config: Config): HttpSettings = {
    val port = config.getInt("port")
    val interface = config.getString("interface")
    val backlog = config.getInt("backlog")
    val requestTimeout = FiniteDuration(config.getDuration("request-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val tls = if (config.hasPath("tls")) Some(TlsSettings.parse(config.getConfig("tls"))) else None
    new HttpSettings(interface, port, backlog, requestTimeout, tls)
  }
}

sealed trait TlsClientAuth
case object TlsClientAuthRequired extends TlsClientAuth
case object TlsClientAuthRequested extends TlsClientAuth
case object TlsClientAuthIgnored extends TlsClientAuth

case class TlsSettings(clientAuth: TlsClientAuth,
                       keystore: String,
                       truststore: String,
                       keystorePassword: String,
                       truststorePassword: String,
                       keymanagerPassword: String)

object TlsSettings {
  def parse(config: Config): TlsSettings = {
    val keystore = config.getString("key-store")
    val truststore = config.getString("trust-store")
    val password = if (config.hasPath("password")) config.getString("password") else null
    val keystorePassword = if (config.hasPath("key-store-password")) config.getString("key-store-password") else password
    val truststorePassword = if (config.hasPath("trust-store-password")) config.getString("trust-store-password") else password
    val keymanagerPassword = if (config.hasPath("keymanager-password")) config.getString("keymanager-password") else password
    val clientAuth = if (config.hasPath("client-auth")) {
      config.getString("tls-client-auth").toLowerCase match {
        case "required" => TlsClientAuthRequired
        case "requested" => TlsClientAuthRequested
        case "ignored" => TlsClientAuthIgnored
        case unknown => throw new Exception("unknown tls client auth mode '%s'".format(unknown))
      }
    } else TlsClientAuthRequested
    new TlsSettings(clientAuth, keystore, truststore, keystorePassword, truststorePassword, keymanagerPassword)
  }
}
