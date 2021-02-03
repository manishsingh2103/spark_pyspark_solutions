package project_1_package
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.format.IntType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{avg, col, concat, count, dense_rank, lit, lower, rank,max}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DecimalType, DoubleType, StringType, StructField, StructType}

object main {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkConf= new SparkConf()
    sparkConf.setAppName("project_1")
    sparkConf.setMaster("local")
    val sc=new SparkContext(sparkConf)
    val spark = SparkSession.builder().master("local").appName("project_1").getOrCreate()
    println("hello world !!")
//    val intarr = Seq( Row("numbers",List("Manish","Kumar","Singh")))
//    val intarrrdd : RDD[Row]=sc.parallelize(intarr,4)
//    println(intarrrdd)
    val schema = new StructType()
      .add(StructField("appid", StringType, true))
      .add(StructField("first_name",StringType,true))
      .add(StructField("last_name",StringType,true))
      .add(StructField("address",StringType,true))
      .add(StructField("gender",StringType,true))
      .add(StructField("phone_no",StringType,true))
//      .add(StructField("val2", DoubleType, true))

    var intarrrdd=spark.read
      .option("header", "false")
      .option("delimiter", " ")
      .option("inferSchema", "true")
      .format("csv")
      .schema(schema)
      .load("resources/*.txt")
    import spark.implicits._
    intarrrdd=intarrrdd
      .withColumn("lower_name", lower(concat(col("first_name"),col("last_name"))))

    intarrrdd.orderBy("lower_name")show(false)

    val w = Window.partitionBy("lower_name")
    val wa = Window.partitionBy("lower_name","address")
    val wg = Window.partitionBy("lower_name","gender")
    val wp = Window.partitionBy("lower_name","phone_no")

    intarrrdd=intarrrdd
      .withColumn("count_a", count("lower_name").over(wa))
      .withColumn("count_g", count("lower_name").over(wg))
      .withColumn("count_p", count("lower_name").over(wp))
      .withColumn("score", col("count_a")+col("count_g")+col("count_p"))
      .withColumn("maxscore", max("score").over(w))
      .filter(col("score")===col("maxscore"))
      .select("appid","first_name","last_name","address","gender","phone_no","score")
      .distinct()

    intarrrdd.show(false)
//    ##################################################################################################
//    val columns = Seq("language","users_count")
//    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
//    val rdd = spark.sparkContext.parallelize(data)
//    var dfFromData = spark.createDataFrame(rdd).toDF(columns:_*)
//    dfFromData.show(false)
//    val col1=dfFromData.schema.fields.filter(x => x.dataType == StringType)
//    val cols=dfFromData.schema.fields
//    val names=col1.map(_.name)
//    print(col1.mkString(","))
//    col1.foreach(x => println(x))
//    cols.foreach(x => println(x))
//    println(cols.mkString(","))
//    StructField(language,StringType,true),StructField(users_count,StringType,true)
//
//   names.foreach(x => println(x))




  }
}
