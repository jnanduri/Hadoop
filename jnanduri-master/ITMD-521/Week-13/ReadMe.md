# Jayanth Nanduri

-----------------------------------

Spark Installation Screenshot

![spark installation](https://user-images.githubusercontent.com/26098043/33242453-ec2d1160-d29a-11e7-8592-0a8a0e006ab8.png)

------------------------------------------------------------------------------------------------------------------------------------------------------------

Displaying first 20 rows for each data set

![result](https://user-images.githubusercontent.com/26098043/33242543-5b72bb1e-d29c-11e7-8303-22bfb6fefc85.png)

<img width="960" alt="1" src="https://user-images.githubusercontent.com/26098043/33242550-780ae42c-d29c-11e7-8416-0fa69b352bf4.PNG">


Code:

spark-shell --packages com.databricks:spark-avro_2.11:4.0.0

import com.databricks.spark.avro._

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

import sqlContext.implicits._

val products = sqlContext.read.avro("hdfs://localhost/user/root/spark_demo/scala/")

products.show

products.toDF()

products.printSchema()

val prodrows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = products.rdd

val categories = sqlContext.read.avro("hdfs://localhost/user/root/spark_demo/")

categories.show

categories.toDF()

categories.printSchema()

categories.take(20).foreach(println)

val catrows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = categories.rdd

------------------------------------------------------------------------------------------------------------------------------------------------------------

Filtered the dataset using RDD, which has products price less than 100 USD

<img width="960" alt="2" src="https://user-images.githubusercontent.com/26098043/33242621-9ecb6b44-d29d-11e7-968e-bb654e9079d6.PNG">

Code:


import com.databricks.spark.avro._

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

val products = sqlContext.read.avro("hdfs://localhost/user/root/spark_demo/scala/")

val prodrows1 = products.as[(String,String,String,String,String)].rdd

val clean = prodrows1.filter(_._4.toString != "")

val hundred = clean.filter(_._4.toFloat < 100)

hundred.foreach(println)

hundred.saveAsTextFile("hdfs://localhost/user/root/spark_demo/Result/Result_0")

Top 10 products prices in each category

![top10](https://user-images.githubusercontent.com/26098043/33242569-b5621c6e-d29c-11e7-83dd-4ab24b3f2b26.png)

Code:

import com.databricks.spark.avro._

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

val join = categories.join(products, categories("category_id") === products("_2"))

import org.apache.spark.sql.functions.{row_number, max, broadcast}

import org.apache.spark.sql.expressions.Window

val prodorder = Window.partitionBy($"category_id").orderBy($"_4".desc)

val top10 = join.withColumn("rn", row_number.over(prodorder)).where($"rn" <= 10).drop("rn").filter($"_4" < 100)

top10.coalesce(1).write.format("com.databricks.spark.csv").save("hdfs://localhost/user/spark/Result_2/")

scala> dfteens.printSchema

root

 |-- category_id: string (nullable = true)

 |-- _3: string (nullable = true)

 |-- _4: string (nullable = true)

scala> val newNames = Seq("c_id", "name", "price")

newNames: Seq[String] = List(c_id, name, price)

scala> val dfRenamed = dfteens.toDF(newNames: _*)

dfRenamed: org.apache.spark.sql.DataFrame = [c_id: string, name: string ... 1 more field]

scala> dfRenamed.printSchema

root

 |-- c_id: string (nullable = true)

 |-- name: string (nullable = true)

 |-- price: string (nullable = true)



------------------------------------------------------------------------------------------------------------------------------------------------------------



 highest and lowest product price in each category



![min and max](https://user-images.githubusercontent.com/26098043/33242578-dc9f9dec-d29c-11e7-93ee-9eb896c90126.png)



import com.databricks.spark.avro._

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

import sqlContext.implicits._

import org.apache.spark.sql.functions.{row_number, max, broadcast}

import org.apache.spark.sql.expressions.Window

val products = sqlContext.read.avro("hdfs://localhost/user/root/spark_demo/scala/")

val categories = sqlContext.read.avro("hdfs://localhost/user/root/spark_demo/")

val prodjoin = categories.join(products, categories("category_id") === products("_2"))

val prodcatorderdesc = Window.partitionBy($"category_id").orderBy($"_4".desc)

val prodcatorderasc = Window.partitionBy($"category_id").orderBy($"_4".asc)

val top1prodcat = prodjoin.withColumn("rn", row_number.over(prodcatorderdesc)).where($"rn" === 1).drop("rn").filter($"_4" < 100)

val down1prodcat = prodjoin.withColumn("rn", row_number.over(prodcatorderasc)).where($"rn" === 1).drop("rn").filter($"_4" < 100)

val one=top1prodcat.select($"category_id",$"category_name",$"_1",$"_2",$"_3", $"_4", $"_5")

val cat1 = one.withColumnRenamed("_1","number1")

val cat2 = cat1.withColumnRenamed("_2","number2")

val cat3 = cat2.withColumnRenamed("_3","highest_product_name")

val cat4 = cat3.withColumnRenamed("_4","highest_product_price")

val cat5 = cat4.withColumnRenamed("_5","images")

val fin1 = cat5.select($"category_id", $"highest_product_name", $"highest_product_price")

val two=down1prodcat.select($"category_id",$"category_name",$"_1",$"_2",$"_3", $"_4", $"_5")

val cat11 = two.withColumnRenamed("_1","number1")

val cat22 = cat11.withColumnRenamed("_2","number2")

val cat33 = cat22.withColumnRenamed("_3","lowest_product_name")

val cat44 = cat33.withColumnRenamed("_4","lowest_product_price")

val cat55 = cat44.withColumnRenamed("_5","images")

val cat555 = cat55.withColumnRenamed("category_id","cid")

val cat5555 = cat555.withColumnRenamed("category_name","cname")

val fin2 = cat5555.select($"cid",$"cname", $"lowest_product_name", $"lowest_product_price")

val finaljoin = fin2.join(fin1, fin2("cid") === fin1("category_id"))

val finaloutput = finaljoin.select($"cname",$"highest_product_name",$"highest_product_price",$"lowest_product_name",$"lowest_product_price")

finaloutput.write.format("com.databricks.spark.avro").save("hdfs://localhost/user/spark_demo/Result_2.avro")

finaloutput.coalesce(1).write.avro("/home/vagrant/Result_2.avro")






