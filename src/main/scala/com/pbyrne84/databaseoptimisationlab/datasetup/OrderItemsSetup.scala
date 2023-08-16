package com.pbyrne84.databaseoptimisationlab.datasetup

class OrderItemsSetup(implicit val connection: java.sql.Connection) {
  import anorm._
  import anorm.SqlParser._
  import scala.collection.parallel.CollectionConverters._

  def generateOrderItems(): Unit = {

    val orderIds = getCurrentOrderIds
    val itemIds = getCurrentItemIds

    val orderIdGroups = orderIds.grouped(10000)
    orderIdGroups.zipWithIndex.foreach {
      case (subGroupOrderIds, currentIndex) =>
        println(s"Runnning $currentIndex of ${orderIdGroups.size}")

        runBatch(subGroupOrderIds, itemIds)
    }

  }

  private def runBatch(orderIds: List[Int], itemIds: List[Int]): Unit = {
    //language=SQL
    val insertSql =
      """
        |INSERT INTO customer_order_item(order_id, item_id)
        |VALUES ({order_id}, {item_id})
        |""".stripMargin

    println("generating order items")
    val batchedParams = generateBindParams(orderIds, itemIds).grouped(1000).toList

    println("running inserts for order items")
    batchedParams.zipWithIndex.par.foreach {
      case (params: Seq[Seq[NamedParameter]], index) =>
        println(s"Inserting order items group $index of ${batchedParams.length}")
        val batchOperation = BatchSql(insertSql, params.head, params.tail: _*)
        batchOperation.execute()
    }
  }

  private def getCurrentOrderIds: List[Int] = {
    //language=SQL
    val selectStatement = "SELECT id FROM customer_order ORDER BY id"
    SQL(selectStatement)
      .as(get[Int]("id").*)
  }

  private def getCurrentItemIds: List[Int] = {
    //language=SQL
    val selectStatement = "SELECT id FROM item"
    SQL(selectStatement)
      .as(get[Int]("id").*)
  }

  private def generateBindParams(orderIds: List[Int], itemIds: List[Int]): List[Seq[NamedParameter]] = {
    orderIds.par.flatMap { orderId =>
      val showDebug = orderId == 1 || orderId % 1000 == 0
      if (showDebug) {
        println(s"Generating for order id $orderId")
      }
      val value = generatePerOrderId(orderId, itemIds)

      if (showDebug) {
        println(s"generated ${value.size} \n")
      }
      value
    }.toList
  }

  private def generatePerOrderId(orderId: Int, itemIds: List[Int]): List[Seq[NamedParameter]] = {
    val itemModulus = orderId % 100000 + 10
    val filteredItemIds = itemIds.filter(_ % itemModulus == 0)
    filteredItemIds.par.map { filteredItemId =>
      Seq[NamedParameter]("order_id" -> orderId, "item_id" -> filteredItemId)
    }.toList
  }

}
