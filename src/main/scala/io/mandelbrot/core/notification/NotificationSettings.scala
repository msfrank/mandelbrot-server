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

import com.typesafe.config.{ConfigObject, ConfigValueType, Config}
import scala.collection.JavaConversions._

import io.mandelbrot.core.{ServerConfigException, ServiceExtension}
import scala.collection.mutable
import org.slf4j.LoggerFactory
import java.io.File
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

/**
 *
 */
case class NotifierSettings(plugin: String, settings: Option[Any])

/**
 *
 */
case class NotificationSettings(contacts: Map[String,Contact],
                                groups: Map[String,ContactGroup],
                                notifiers: Map[String,NotifierSettings],
                                rules: NotificationRules,
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
        if (!ServiceExtension.pluginImplements(plugin, classOf[Notifier]))
          throw new ServerConfigException("%s is not recognized as a Notifier plugin".format(plugin))
        val settings = if (notifierConfig.hasPath("plugin-settings")) {
          notifierContacts.get(name) match {
            case Some(params) =>
              ServiceExtension.makePluginSettings(plugin, notifierConfig.getConfig("plugin-settings"), Some(params.toMap))
            case None =>
              ServiceExtension.makePluginSettings(plugin, notifierConfig.getConfig("plugin-settings"), Some(Map.empty[Contact, Config]))
          }
        } else None
        Some(name -> NotifierSettings(plugin, settings))
      case unknown =>
        None
    }.toMap

    // parse notification rules
    val rulesFile = new File(config.getString("notification-rules-file"))
    val rules = NotificationRules.parse(rulesFile, contacts, groups)

    // parse snapshot configuration
    val snapshotInitialDelay = FiniteDuration(config.getDuration("snapshot-initial-delay", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
    val snapshotInterval = FiniteDuration(config.getDuration("snapshot-interval", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

    new NotificationSettings(contacts, groups, notifiers, rules, snapshotInitialDelay, snapshotInterval)
  }
}

