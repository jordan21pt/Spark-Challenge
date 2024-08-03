package org.example

import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object Main {

  // Ex 1
  def criaDataFrameComEstrutura(df: DataFrame): DataFrame = {

    // Substituir 'nan' por 0.0 na coluna Sentiment_Polarity
    val cleanedDf = df.withColumn("Sentiment_Polarity", when(col("Sentiment_Polarity") === "nan", lit(0.0)).otherwise(col("Sentiment_Polarity")))

    val cleanedDfColEspecificas = cleanedDf.select("App", "Sentiment_Polarity")

    cleanedDfColEspecificas
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").as("Average_Sentiment_Polarity"))
      .orderBy("App")
  }

  //Ex 2:
  // Falta alterar o nome do ficheiro para o pedido...
  def criaDataFramePart_2(df: DataFrame): Unit = {

    val df_filtered = df.na.drop(Seq("Rating")).filter(col("Rating") >= 4.0).orderBy(col("Rating").desc)

    df_filtered.coalesce(1) //Este coalesce(1) faz com que seja gerado apenas 1 ficheiro... pode dar asneiro se os dados forem muito grandes
      .write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "§")
      .mode("overwrite")
      .save("src/main/scala/org/example/Outputs/best_apps.csv")

  }

  // Função para converter tamanho de String para Double
  def convertSize(size: String): Double = {
    if (size.endsWith("M")) size.stripSuffix("M").toDouble / 8
    else if (size.endsWith("k")) size.stripSuffix("k").toDouble / 1024
    else Double.NaN
  }

  // Função para converter preço de String para Double em euros
  def convertPrice(price: String): Double = {
    if (price.startsWith("$")) price.stripPrefix("$").toDouble * 0.9
    else 0.0
  }

  val sizeUDF = udf[Double, String](convertSize)
  val priceUDF = udf[Double, String](convertPrice)

  //Ex 3:
  def criaDataFramePart_3(df: DataFrame): DataFrame = {

    // Definindo a janela para particionar por "APP" e ordenar por "Reviews"
    val windowSpec = Window.partitionBy("APP").orderBy(desc("Reviews"))

    // Adicionando uma coluna de classificação baseada no valor "Reviews"
    val dfWithRank = df.withColumn("rank", rank().over(windowSpec))

    // Filtrando para obter apenas a linha com a classificação 1 (ou seja, o valor máximo)
    val dfMaxReviews = dfWithRank.filter(col("rank") === 1).drop("rank")

    // Agrupar por "APP" e adicionar categorias em um array, mantendo todas as outras colunas
    val result = dfMaxReviews.groupBy("APP")
      .agg(
        collect_set("Category").as("Categories"),
        max("Reviews").as("Reviews"),
        first("Rating").as("Rating"),
        first("Size").as("Size"),
        first("Installs").as("Installs"),
        first("Type").as("Type"),
        first("Price").as("Price"),
        first("Content Rating").as("Content_Rating"),
        first("Genres").as("Genres"),
        first("Last Updated").as("Last_Updated"),
        first("Current Ver").as("Current_Version"),
        first("Android Ver").as("Minimum_Android_Version")
      )

    val dfConvertSize = result.withColumn("Size", sizeUDF(col("Size")))
    val dfConvertPrice = dfConvertSize.withColumn("Price", priceUDF(col("Price")))
    val dfConvertGenres = dfConvertPrice.withColumn("Genres", split(col("Genres"), ";").cast("array<string>"))
    val dfConvertDates = dfConvertGenres.withColumn(
      "Last_Updated",
      to_date(unix_timestamp(col("Last_Updated"), "MMMM dd, yyyy").cast("timestamp"))
    )

    dfConvertDates
  }

  def criaDataFramePart_4(df_1: DataFrame, df_3: DataFrame): DataFrame = {

    val joinedDF = df_3.join(df_1, Seq("App"), "outer")

    joinedDF.write
      .option("comprenssion", "gzip")
      .mode("overwrite")
      .parquet("src/main/scala/org/example/Outputs/googleplaystore_cleaned")

    joinedDF
  }

  // Ex5
  def criaDataFramePart_5(df: DataFrame): DataFrame = {

    val cleanedDf = df.withColumn("Rating", when(col("Rating") === "nan", lit(0.0)).otherwise(col("Rating")))
    val explodedDF = cleanedDf.withColumn("Genre", explode(col("Genres")))

    // Agrupar por gênero e calcular as agregações necessárias
    val aggregatedDF = explodedDF.groupBy("Genre")
      .agg(
        count("App").alias("NumberOfApplications"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )

    aggregatedDF.write
      .option("comprenssion", "gzip")
      .mode("overwrite")
      .parquet("src/main/scala/org/example/Outputs/googleplaystore_metrics")

    aggregatedDF
  }

  def main(args: Array[String]): Unit = {

    // Criar uma sessão Spark
    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local[*]")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()


    // Abrir os ficheiros uma vez logo ao iniciar o programa... é mais económico...
    // como nao tinha a certeza o que era suposto fazer com as linhas mal formadas, decidi eleminá-las
    val df_gps = spark.read
      .option("header", "true")
      .option("quote", "\"")  // Define o caractere de citação como aspas duplas
      .option("escape", "\"") // Define o caractere de escape como aspas duplas
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")  // Elimina linhas malformadas
      .csv("src/main/scala/org/example/google-play-store-apps/googleplaystore.csv")

    val df_gpsur = spark.read
        .option("header", "true")
        .option("quote", "\"")  // Define o caractere de citação como aspas duplas
        .option("escape", "\"") // Define o caractere de escape como aspas duplas
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")  // Elimina linhas malformadas
        .csv("src/main/scala/org/example/google-play-store-apps/googleplaystore_user_reviews.csv")

    //EX1
    val df_1 = criaDataFrameComEstrutura(df_gpsur)
    df_1.show()

    //EX2
    criaDataFramePart_2(df_gps)

    //EX3
    val df_3 = criaDataFramePart_3(df_gps)
    df_3.show()

    //EX4
    val df_4 = criaDataFramePart_4(df_1, df_3)
    df_4.show()

    //EX5
    val df_5 = criaDataFramePart_5(df_4)
    df_5.show(200)


    spark.stop()
  }
}
