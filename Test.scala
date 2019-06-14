/*object Test {

}
*/

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext





object Test{
  def main(args:Array[String])={

    val conf = new SparkConf()
      .setAppName("Test")
      .setMaster("local")

    val sc = new SparkContext(conf)


    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)



    val sql= """(select * from db.productcategories)  """

    val df = sqlcontext.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mysql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", s"( $sql ) t")
      .option("user", "root")
      .option("password", "asmitas28")
      .load()



    df.show()


    //Store on hdfs


    df.repartition(3)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("hdfs://localhost:9000/productcategories")





    val sql2="""(select DetailProductID, DetailQuantity from db.orderdetails)"""

    val df2 = sqlcontext.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mysql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", s"( $sql2 ) t")
      .option("user", "root")
      .option("password", "asmitas28")
      .load()

    df2.show()



    //Store on hdfs


    df2.repartition(3)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("hdfs://localhost:9000/orderdetails")




    val sql3="""(select ProductID, ProductStock, ProductCategoryID from db.products ) """

    val df3 = sqlcontext.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mysql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", s"( $sql3 ) t")
      .option("user", "root")
      .option("password", "asmitas28")
      .load()

    df3.show()

    //Store on hdfs

    df3.repartition(3)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("hdfs://localhost:9000/products")

    df3.show()


    val sql4="""(select Item_Type, Outlet_Identifier, stocks from sd.mytable ) """

    val df4 = sqlcontext.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mysql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", s"( $sql4 ) t")
      .option("user", "root")
      .option("password", "asmitas28")
      .load()

    //df4.show()

    //Store on hdfs

    df4.repartition(3)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("hdfs://localhost:9000/item")



    df4.show()

    val small_df = df4.limit(100)
    small_df.show()



  }
}