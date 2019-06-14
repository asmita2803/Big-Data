/*
object Load_data {

}

*/

//package main.scala
import org.apache.spark.SparkConf
// import org.apache.spark.sql.functions.col
import org.apache.spark.SparkContext

object Load_data {
  def main(args:Array[String])={
    val conf = new SparkConf()
      .setAppName("Test")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)

    //Load all data from hdfs

    val category = sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("delimiter", ",").load("hdfs://localhost:9000/productcategories")
    category.show()


    val order = sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("delimiter", ",").load("hdfs://localhost:9000/orderdetails")
    order.show()

    val product = sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("delimiter", ",").load("hdfs://localhost:9000/products")
    product.show()

    val stock = sqlcontext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("delimiter", ",").load("hdfs://localhost:9000/item")
    stock.show()

    //Register dataframes as tables

    order.registerTempTable("orders")
    product.registerTempTable("products")

    //Query to get required data from both tables

    val join= sqlcontext.sql("SELECT ord.DetailQuantity,ord.DetailProductID,p.ProductStock,p.ProductCategoryID FROM orders ord, products p where ord.DetailProductID = p.ProductID")
    join.show()

    //Register dataframes as tables

    join.registerTempTable("orig")
    category.registerTempTable("cate")

    //Query to get required data from both tables

    val f_join= sqlcontext.sql("SELECT o.*,c.CategoryName FROM orig o, cate c where o.ProductCategoryID = c.CategoryID")


    f_join.show()


    stock.registerTempTable("ship")
    f_join.registerTempTable("data")

    val final_join = sqlcontext.sql("SELECT d.DetailProductID, s.Outlet_Identifier FROM data d, ship s where d.ProductStock=s.stocks")


    //Store filtered data on hdfs
    final_join.repartition(3)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("hdfs://localhost:9000/filtered_data")


    //Store amount to be shipped
    val quantity = sqlcontext.sql("SELECT Item_Type, Outlet_Identifier, stocks FROM ship where stocks<51")
    quantity.show()

    quantity.registerTempTable("quan")

    val amt = sqlcontext.sql("SELECT Item_Type, Outlet_Identifier, (100-stocks) as Send from quan")
    amt.show()


  }
}
