package io.mandelbrot.core

import akka.actor.{Extension, Props}
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{currentMirror => cm}

trait ServiceExtension extends Extension {

  /**
   * given a fully-qualified class name, return a Props instance instantiated
   * by invoking the props() method of the specified class companion object.
   */
  def findPropsFor(pluginClassName: String): Props = {
    val pluginClass = Class.forName(pluginClassName)
    val classSymbol = cm.classSymbol(pluginClass)
    val moduleSymbol = classSymbol.companionSymbol.asModule
    val moduleMirror = cm.reflectModule(moduleSymbol)
    val instanceMirror = cm.reflect(moduleMirror.instance)
    val propsSymbol = moduleMirror.symbol.typeSignature.member(newTermName("props")).asMethod
    val methodMirror = instanceMirror.reflectMethod(propsSymbol)
    val props = methodMirror.apply().asInstanceOf[Props]
    props
  }
}
