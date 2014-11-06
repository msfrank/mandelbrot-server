package io.mandelbrot.persistence.cassandra

trait RegistrarStatements {

  val tableName: String

  def createRegistryTable =
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  uri text PRIMARY KEY,
       |  registration text,
       |  lsn bigint,
       |  last_update timestamp,
       |  joined_on timestamp)
       |WITH COMPACT STORAGE
     """.stripMargin

  def registerProbeSystemStatement =
    s"""
       |INSERT INTO $tableName (uri, registration, lsn, last_update, joined_on)
       |VALUES (?, ?, 1, ?, ?) IF NOT EXISTS
     """.stripMargin

  def updateProbeSystemStatement =
    s"""
       |UPDATE $tableName
       |SET registration = ?, lsn = ?, last_update = ?
       |WHERE uri = ?
       |IF lsn = ?
     """.stripMargin

  def deleteProbeSystemStatement =
    s"""
       |DELETE FROM $tableName
       |WHERE uri = ?
       |IF lsn = ?
     """.stripMargin

  def getProbeSystemStatement =
    s"""
       |SELECT registration, lsn FROM $tableName
       |WHERE uri = ?
     """.stripMargin

  def listProbeSystemsStatement =
    s"""
       |SELECT uri, lsn, last_update, joined_on FROM $tableName
       |LIMIT ?
     """.stripMargin

}
