package com.pbyrne84.databaseoptimisationlab.datasetup

import anorm.{BatchSql, NamedParameter}

import java.time.Instant

class CustomerOrderSetup(implicit val connection: java.sql.Connection) {
  import scala.collection.parallel.CollectionConverters._

  /**
    * Generates different counts of orders per customer to fake a representation of a live system,
    */
  def setupOrders(customerIds: List[Int]): Unit = {
    val (_, params: Seq[Seq[NamedParameter]]) = customerIds.par.foldLeft((1, List.empty[Seq[NamedParameter]])) {
      case ((currentOrderId, currentParams: Seq[Seq[NamedParameter]]), customerId: Int) =>
        val (nextId, newParams: Seq[Seq[NamedParameter]]) = {
          if (currentOrderId % 1000 == 0) {
            println(s"generating order $currentOrderId")
          }

          generateBulkInsertValuesForCustomer(customerId, currentOrderId)
        }
        (nextId, currentParams ++ newParams)
    }

    //language=SQL
    val insertStatement =
      """
          |INSERT INTO customer_order (id, customer_id, created)
          |VALUES ({id},{customer_id},{created})""".stripMargin

    params.grouped(1000).toList.par.foreach { subChunk: Seq[Seq[NamedParameter]] =>
      val batchOperation = BatchSql(insertStatement, subChunk.head, subChunk.tail: _*)
      batchOperation.execute()
    }
  }

  private def generateBulkInsertValuesForCustomer(customerId: Int,
                                                  currentOrderId: Int): (Int, Seq[Seq[NamedParameter]]) = {
    val numberOfOrders = customerId % 10
    createOrderBulkInsert(currentOrderId, numberOfOrders, customerId)
  }

  private def createOrderBulkInsert(startingOrderId: Int,
                                    numberOfOrderForCustomer: Int,
                                    customerId: Int): (Int, Seq[Seq[NamedParameter]]) = {
    val params = (0 until numberOfOrderForCustomer).par.map { customerOrderIndex =>
      val created = generateCreatedDate(customerOrderIndex)
      // Would be nice to maybe use a sequence but there is not documentation on doing bulk inserts with anorm and sequences

      val orderId = customerOrderIndex + startingOrderId
      Seq[NamedParameter]("id" -> orderId, "customer_id" -> customerId, "created" -> created)
    }.toList

    val nextOrderId = params.size + startingOrderId
    nextOrderId -> params
  }

  /**
    * Orders happen in different order to item and customer creation. We are playing with modulus to mess with the order
    *
    * @param id
    * @return
    */
  private def generateCreatedDate(id: Int): Instant = {
    Instant.now.minusSeconds(id % 140 + id)
  }
}
