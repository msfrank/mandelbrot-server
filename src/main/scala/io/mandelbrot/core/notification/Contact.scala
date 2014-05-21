package io.mandelbrot.core.notification

/**
 *
 */
case class Contact(id: String, name: String, metadata: Map[String,String])

/**
 *
 */
case class ContactGroup(id: String, name: String, metadata: Map[String,String], contacts: Set[Contact])

/**
 *
 */
case class NotifyContact(contact: Contact, notification: Notification)
