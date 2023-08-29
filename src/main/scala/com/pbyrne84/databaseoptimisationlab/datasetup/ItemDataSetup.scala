package com.pbyrne84.databaseoptimisationlab.datasetup

import java.time.Instant

class ItemDataSetup(itemCount: Int)(implicit val connection: java.sql.Connection) {

  import anorm._
  import scala.collection.parallel.CollectionConverters._

  def generateItems(): Unit = {
    val itemsToCreateAtATime = 1000
    val inserts = for {
      index <- (1 to itemCount) by itemsToCreateAtATime // we will create an insert of 100 items at a time
      value = createCustomerBulkInsert(index, itemsToCreateAtATime)
      //language=SQL
      batchOperation = BatchSql(
        "INSERT INTO item (id, name, price, created, updated) VALUES ({id},{name},{price},{created},{updated})",
        value.head,
        value.tail: _*
      )
    } yield batchOperation

    inserts.zipWithIndex.foreach {
      case (insert, index) =>
        println(s"processing batch $index")
        println("completed insert count " + insert.execute().toList.size)
    }

  }

  private def createCustomerBulkInsert(index: Int, increment: Int): Seq[Seq[NamedParameter]] = {
    (0 until increment).par.map { subIndex =>
      val id = index + subIndex
      val name = createItemName(id)
      val created = Instant.now
      val updated = generateUpdatedDate(id)

      val price = generatePrice(id)

      Seq[NamedParameter]("id" -> id, "name" -> name, "price" -> price, "created" -> created, "updated" -> updated)
    }.toList
  }

  private def createItemName(id: Int) = {
    s"item $id"
  }

  /**
    * Updates happen in different order to creation. We are playing with modulus to mess with the order
    *
    * @param id
    * @return
    */
  private def generateUpdatedDate(id: Int): Instant = {
    Instant.now.minusSeconds(id % 111 + id)
  }
  private def generatePrice(id: Int) = {
    val pence = id % 100
    val pounds = 100000000 - id + (1000 % id)

    BigDecimal(s"${pounds}.${pence}")
  }

}
