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

package io.mandelbrot.core.notification

import akka.actor.Props
import com.typesafe.config.{ConfigFactory, ConfigObject, ConfigValueType, Config}
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import java.io.File

import io.mandelbrot.core.model._
import io.mandelbrot.core.ServerConfigException

/**
 *
 */
case class NotifierSettings(plugin: String, props: Props)

/**
 *
 */
case class NotificationSettings(contacts: Map[String,Contact],
                                groups: Map[String,ContactGroup],
                                notifiers: Map[String,NotifierSettings],
                                rules: NotificationRules,
                                cleanerInitialDelay: FiniteDuration,
                                cleanerInterval: FiniteDuration,
                                staleWindowOverlap: FiniteDuration,
                                snapshotInitialDelay: FiniteDuration,
                                snapshotInterval: FiniteDuration)

/**
 *
 */
object NotificationSettings {
  def parse(config: Config): NotificationSettings = {

    val logger = LoggerFactory.getLogger(classOf[NotificationSettings])

    val notifierContacts = new mutable.HashMap[String,mutable.HashMap[Contact,Config]]()

    // parse contacts configuration
    val contacts = config.getConfig("contacts").root.flatMap {
      case (id,contactConfigValue) if contactConfigValue.valueType() == ConfigValueType.OBJECT =>
        val contactConfig = contactConfigValue.asInstanceOf[ConfigObject].toConfig
        // create the contact
        val contactName = contactConfig.getString("contact-name")
        val contactMetadata = if (!contactConfig.hasPath("contact-metadata")) Map.empty[String,String] else {
          contactConfig.getConfig("contact-metadata").root().unwrapped().map { case (key,value) => key -> value.toString }.toMap
        }
        val contact = Contact(id, contactName, contactMetadata)
        // store the notifier-specific config for each contact
        contactConfig.getConfig("notifier").root.foreach {
          case (notifier,notifierConfigValue) if notifierConfigValue.valueType() == ConfigValueType.OBJECT =>
            val notifierConfig = notifierConfigValue.asInstanceOf[ConfigObject].toConfig
            notifierContacts.get(notifier) match {
              case None =>
                val contactsConfig = new mutable.HashMap[Contact,Config]()
                contactsConfig.put(contact, notifierConfig)
                notifierContacts.put(notifier, contactsConfig)
              case Some(contactsConfig) =>
                contactsConfig.put(contact, notifierConfig)
                notifierContacts.put(notifier, contactsConfig)
            }
          case unknown => // do nothing
        }
        Some(id -> contact)
      case unknown =>
        None
    }.toMap

    // parse contact groups
    val groups = config.getConfig("groups").root.flatMap {
      case (id, groupConfigValue) if groupConfigValue.valueType() == ConfigValueType.OBJECT =>
        val groupConfig = groupConfigValue.asInstanceOf[ConfigObject].toConfig
        // create the group
        val groupName = groupConfig.getString("group-name")
        val groupMetadata = if (!groupConfig.hasPath("group-metadata")) Map.empty[String, String]
        else {
          groupConfig.getConfig("group-metadata").root().unwrapped().map { case (key, value) => key -> value.toString}.toMap
        }
        val groupMembers = groupConfig.getStringList("group-members").flatMap(contacts.get).toSet
        Some(id -> ContactGroup(id, groupName, groupMetadata, groupMembers))
      case unknown =>
        None
    }.toMap

    // parse notifier configuration
    val notifiers = config.getConfig("notifiers").root.flatMap {
      case (name,configValue) if configValue.valueType() == ConfigValueType.OBJECT =>
        val notifierConfig = configValue.asInstanceOf[ConfigObject].toConfig
        val plugin = notifierConfig.getString("plugin")
        val pluginSettings = if (notifierConfig.hasPath("plugin-settings")) notifierConfig.getConfig("plugin-settings") else ConfigFactory.empty()
        val props = NotificationEmitter.extensions.get(plugin) match {
          case None =>
            throw new ServerConfigException("%s is not recognized as a NotificationEmitterExtension".format(plugin))
          case Some(extension) =>
            extension.props(extension.configure(pluginSettings))
        }
        Some(name -> NotifierSettings(plugin, props))
      case unknown =>
        None
    }.toMap

    // parse notification rules
    val rulesFile = new File(config.getString("notification-rules-file"))
    val rules = NotificationRules.parse(rulesFile, contacts, groups)

    // parse window cleaner configuration
    val cleanerInitialDelay = FiniteDuration(config.getDuration("cleaner-initial-delay", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val cleanerInterval = FiniteDuration(config.getDuration("cleaner-interval", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val staleWindowOverlap = FiniteDuration(config.getDuration("stale-window-overlap", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

    // parse snapshot configuration
    val snapshotInitialDelay = FiniteDuration(config.getDuration("snapshot-initial-delay", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val snapshotInterval = FiniteDuration(config.getDuration("snapshot-interval", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

    new NotificationSettings(contacts,
                             groups,
                             notifiers,
                             rules,
                             cleanerInitialDelay,
                             cleanerInterval,
                             staleWindowOverlap,
                             snapshotInitialDelay,
                             snapshotInterval)
  }
}

