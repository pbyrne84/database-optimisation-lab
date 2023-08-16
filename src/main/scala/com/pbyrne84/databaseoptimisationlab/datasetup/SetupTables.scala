package com.pbyrne84.databaseoptimisationlab.datasetup

import org.flywaydb.core.Flyway

object SetupTables {

  val user: String = "postgres"
  val password: String = "docker"
  val url: String = s"jdbc:postgresql://localhost:5432/postgres?user=$user&password=$password&ssl=false"

  def main(args: Array[String]): Unit = {

    val flyway = Flyway.configure.dataSource(url, user, password).load

    flyway.migrate()
  }
}
