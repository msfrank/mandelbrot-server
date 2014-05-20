package io.mandelbrot.core.notification

/**
 *
 */
case class Contact(id: String, name: String, metadata: Map[String,String])

/**
 *
 */
case class ContactGroup(id: String, name: String, contacts: Vector[Contact])

/**
 *
 */
case class NotifyContact(contact: Contact, notification: Notification)
