package io.mandelbrot.persistence.cassandra

trait ArchiverStatements {

  val keyspaceName: String
  val tableName: String

  val createRegistryTable =
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  uri text,
       |  registration text,
       |  lsn bigint,
       |  last_update timestamp,
       |  joined_on timestamp
       |WITH COMPACT STORAGE
     """.stripMargin

  val registerProbeSystemStatement =
    s"""
       |INSERT INTO $keyspaceName.$tableName (uri, registration, lsn, last_update, joined_on)
       |VALUES (?, ?, 1, ?, ?) IF NOT EXISTS
     """.stripMargin


  val updateProbeSystemStatement =
    s"""
       |UPDATE $keyspaceName.$tableName
       |SET registration = ?, lsn = ?, last_update = ?
       |WHERE uri = ?
       |IF lsn = ?
     """.stripMargin


  val deleteProbeSystemStatement =
    s"""
       |DELETE FROM $keyspaceName.$tableName
       |WHERE uri = ?
       |IF lsn = ?
     """.stripMargin

  val getProbeSystemStatement =
    s"""
       |SELECT registration, lsn FROM $keyspaceName.$tableName
       |WHERE uri = ?
     """.stripMargin

  val listProbeSystemsStatement =
    s"""
       |SELECT uri, lsn, last_update, joined_on FROM $keyspaceName.$tableName
       |LIMIT ?
     """.stripMargin

}
