package com.pbyrne84.databaseoptimisationlab.datasetup

import java.sql.DriverManager
import java.time.Instant
import java.util.Properties

object GenerateData {

  import anorm._

  private val databaseProperties = new Properties
  databaseProperties.setProperty("user", SetupTables.user)
  databaseProperties.setProperty("password", SetupTables.password)
  databaseProperties.setProperty("ssl", "false")

  private implicit val connection: java.sql.Connection =
    DriverManager.getConnection(SetupTables.url, databaseProperties)

  private val customerDataSetup: CustomerDataSetup = new CustomerDataSetup(customerCount = 50000000)
  private val itemDataSetup: ItemDataSetup = new ItemDataSetup(itemCount = 1000000)
  private val customerOrderSetup: CustomerOrderSetup = new CustomerOrderSetup
  private val orderItemsSetup: OrderItemsSetup = new OrderItemsSetup

  def main(args: Array[String]): Unit = {

    reset()

    timeCall("Setting up customers") {
      customerDataSetup.generateCustomersFast()
    }

    timeCall("Setting up items") {
      itemDataSetup.generateItems()
    }

    timeCall("Setting up orders") {
      val orderIds = (1 to 500000).filter(id => id % 9 == 0 || id % 4 == 0).toList
      customerOrderSetup.setupOrders(orderIds)
    }

    timeCall("Setting up order items") {
      orderItemsSetup.generateOrderItems()
    }

  }

  private def reset(): Unit = {
//    List("customer_order_item", "customer_order", "item", "customer").foreach { tableName =>
//      println(s"Deleting from $tableName")
//      SQL(s"DELETE FROM $tableName").execute()
//    }
  }

  /**
    * This uses call by name. The parameter gets converted to an anonymous function for lazy execution.
    * You see this pattern in Option.getOrElse so you can Option.getOrElse(throw new Exception)
    * and in play Action ( final def apply(block: => Result): )
    *
    * It is a pattern primarily to make programming interfaces nicer in code, usually library code. It is
    * definitely not for optimisation as that opens it up to be a foot gun where people accidentally fire
    * things more than once (each time they refer to it)
    *
    */
  private def timeCall(reference: String)(call: => Unit): Unit = {
    println(s"Starting '$reference")
    val start = Instant.now()
    call
    val timeTaken = Instant.now().getEpochSecond - start.getEpochSecond
    println(s"'$reference' took $timeTaken seconds\n")

  }

}
