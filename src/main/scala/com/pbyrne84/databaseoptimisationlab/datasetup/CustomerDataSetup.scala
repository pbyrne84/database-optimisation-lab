package com.pbyrne84.databaseoptimisationlab.datasetup

import java.time.Instant

class CustomerDataSetup(customerCount: Int)(implicit val connection: java.sql.Connection) {
  import anorm._
  import scala.collection.parallel.CollectionConverters._

  /**
    * We generate customersToCreateAtATime per query, this reduces round trips to the database and
    * also lets the database try and organise its processes more effectively.
    * Takes about 4-7 seconds on my machine - you can play with customersToCreateAtATime
    */
  def generateCustomersFast(): Unit = {
    val customersToCreateAtATime = 1000
    val groupedIds = ((1 to customerCount) by customersToCreateAtATime).toList
      .grouped(1000)

    groupedIds.foreach { ids =>
      createCustomers(ids, customersToCreateAtATime)
    }
  }

  private def createCustomers(ids: List[Int], customersToCreateAtATime: Int): Unit = {
    val inserts = for {
      index <- ids // we will create an insert of 100 items at a time
      value = createCustomerBulkInsert(index, customersToCreateAtATime)
      //language=SQL
      insertStatement = """
          |INSERT INTO customer (id, name, created, updated)
          |VALUES ({id},{name},{created}, {updated})""".stripMargin

      batchOperation = BatchSql(insertStatement, value.head, value.tail: _*)
    } yield batchOperation

    inserts.zipWithIndex.foreach {
      case (insert, index) =>
        println(s"processing batch $index")
        println("completed insert count " + insert.execute().toList.size)
    }
  }

  private def createCustomerBulkInsert(index: Int, increment: Int): Seq[Seq[NamedParameter]] = {
    (0 until increment).toList.par.map { subIndex =>
      val id = index + subIndex
      val name = createCustomerName(id)
      val updated = generateUpdatedDate(id)

      Seq[NamedParameter]("id" -> id, "name" -> name, "created" -> Instant.now(), "updated" -> updated)
    }.toList
  }

  /**
    * Updates happen in different order to creation. We are playing with modulus to mess with the order
    * @param id
    * @return
    */
  private def generateUpdatedDate(id: Int): Instant = {
    Instant.now.minusSeconds(id % 100 + id)
  }

  private def createCustomerName(id: Int) = {
    s"customer $id"
  }

  /**
    * Attempt to create customers in a slow fashion, database will throttle.
    * Takes about 3-4 minutes on my machine.
    *
    * A good example of changing how you do things can save a lot of time.
    */
  def generateCustomersSlow(): Unit = {
    (1 to customerCount).par.foreach { index =>
      val name = createCustomerName(index)
      val created = Instant.now
      val updated = generateUpdatedDate(index)
      //language=SQL
      val insert = s"""
      | INSERT INTO customer (id, name, created, updated)
      | VALUES ($index,'$name', '$created', '$updated')
      """
      val _ = SQL(insert).executeInsert()
    }
  }
}
