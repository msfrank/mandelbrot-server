package io.mandelbrot.core.notification

/**
 *
 */
case class Contact(name: String, address: String, metadata: Map[String,String])

/**
 *
 */
case class ContactGroup(name: String, contacts: Vector[Contact])

/**
 *
 */
case class NotifyContact(contact: Contact, notification: Notification)
