package io.mandelbrot.core

import com.typesafe.config.Config
import akka.actor.{Extension, Props}
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{currentMirror => cm}

trait ServiceExtension extends Extension {

  /**
   * given a fully-qualified class name and optional service config, return
   * a Props instance instantiated by invoking the props() method of the
   * specified class companion object.  If serviceConfig is defined, then
   * invoke the one-argument form of the props() method, otherwise invoke the 
   * zero-argument form.
   */
  def makeServiceProps(pluginClassName: String, serviceConfig: Option[Any]): Props = {
    val pluginClass = Class.forName(pluginClassName)
    val classSymbol = cm.classSymbol(pluginClass)
    val moduleSymbol = classSymbol.companionSymbol.asModule
    val moduleMirror = cm.reflectModule(moduleSymbol)
    val instanceMirror = cm.reflect(moduleMirror.instance)
    val propsSymbol = moduleMirror.symbol.typeSignature.member(newTermName("props")).asMethod
    val methodMirror = instanceMirror.reflectMethod(propsSymbol)
    val props = serviceConfig match {
      case Some(value) => methodMirror.apply(value).asInstanceOf[Props]
      case None => methodMirror.apply().asInstanceOf[Props]
    }
    props
  }

}

trait ServiceSettings {
  
  /**
   * 
   */
  def makeServiceSettings(pluginClassName: String, config: Config): Option[Any] = {
    val pluginClass = Class.forName(pluginClassName)
    val classSymbol = cm.classSymbol(pluginClass)
    val moduleSymbol = classSymbol.companionSymbol.asModule
    val moduleMirror = cm.reflectModule(moduleSymbol)
    val instanceMirror = cm.reflect(moduleMirror.instance)
    val propsSymbol = moduleMirror.symbol.typeSignature.member(newTermName("settings")).asMethod
    val methodMirror = instanceMirror.reflectMethod(propsSymbol)
    val serviceConfig = methodMirror.apply(config).asInstanceOf[Option[Any]]
    serviceConfig
  }
}