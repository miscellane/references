package com.grey.events

import io.ably.lib.realtime.Channel.MessageListener
import io.ably.lib.realtime.{AblyRealtime, Channel}
import io.ably.lib.types.Message
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.explode
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class AblyWeather(spark: SparkSession) {

  private val keyReader = new KeyReader().keyReader(apiName = "ably")
  private val localSettings = new LocalSettings()

  def ablyWeather(): Unit = {

    import spark.implicits._

    // Schema
    val schema: StructType = new StructType()
      .add("coord", new StructType()
        .add("lon", DoubleType)
        .add("lat", DoubleType)
      )
      .add("weather",
        ArrayType(new StructType()
        .add("id", IntegerType)
        .add("main", StringType)
        .add("description", StringType)
        .add("icon", StringType)
        )
      )
      .add("base", StringType)
      .add("main", new StructType()
        .add("temp", DoubleType)
        .add("feels_like", DoubleType)
        .add("temp_min", DoubleType)
        .add("temp_max", DoubleType)
        .add("pressure", LongType)
        .add("humidity", LongType)
        .add("sea_level", DoubleType)
        .add("grnd_level", DoubleType)
      )
      .add("visibility", LongType)
      .add("wind", new StructType()
        .add("speed", DoubleType)
        .add("deg", IntegerType)
      )
      .add("clouds", new StructType()
        .add("all", LongType)
        .add("name", StringType)
      )
      .add("rain", new StructType()
        .add("1h", DoubleType)
        .add("3h", DoubleType)
      )
      .add("snow", new StructType()
        .add("1h", DoubleType)
        .add("3h", DoubleType)
      )
      .add("dt", LongType)
      .add("sys", new StructType()
        .add("type", LongType)
        .add("id", LongType)
        .add("message", StringType)
        .add("country", StringType)
        .add("sunrise", LongType)
        .add("sunset", LongType)
      )
      .add("timezone", LongType)
      .add("id", LongType)
      .add("name", StringType)
      .add("cod", LongType)


    // Data & Time String Formatter
    val dateTimeFormat = DateTimeFormat.forPattern("yyyyMMdd.mmHHssSSS")


    // Reading-in
    val ablyRealtime: AblyRealtime = new AblyRealtime(keyReader)
    val channel: Channel = ablyRealtime.channels.get("[product:ably-openweathermap/weather]" + "weather:2643741")
    val message: Message = new Message()
    channel.subscribe(new MessageListener {
      override def onMessage(message: Message): Unit = {

        println(message.data)

        // Read-in the message via the message schema.
        val points: Dataset[String] = Seq(message.data.toString).toDS()
        val pointsFrame: DataFrame = spark.read.schema(schema).json(points)
        pointsFrame.printSchema()

        // Explode: explode applies to arrays only; explode(arrays_zip()).
        val pointsFrameExploding = pointsFrame.select($"coord", explode($"weather").as("weather"),
          $"main", $"wind", $"clouds", $"rain", $"snow", $"sys", $"base", $"visibility", $"dt", $"timezone",
          $"id", $"name", $"cod")
        pointsFrameExploding.printSchema()

        // Destructing
        val pointsFrameDestructing = pointsFrameExploding.select($"coord.*",
          $"weather.id".as("weatherID"), $"weather.main".as("weatherMain"),
          $"weather.description".as("weatherDescription"), $"weather.icon".as("weatherIcon"),
          $"main.*", $"wind.speed".as("windSpeed"), $"wind.deg".as("windDeg"),
          $"clouds.all".as("cloudsAll"), $"clouds.name".as("cloudsName"),
          $"rain.1h".as("rain1H"), $"rain.3h".as("rain3H"),
          $"snow.1h".as("snow1H"), $"snow.3h".as("snow3H"),
          $"sys.type".as("sysType"), $"sys.id".as("sysID"), $"sys.message".as("sysMessage"),
          $"sys.country".as("sysCountry"), $"sys.sunrise".as("sysSunsrise"), $"sys.sunset".as("sysSunset"),
          $"base", $"visibility", $"dt".as("dataCalculationTime"), $"timezone",
          $"id".as("cityID"), $"name".as("cityName"), $"cod")

        // Save
        val dateTimeNow: DateTime = DateTime.now()
        val instant: String = dateTimeFormat.print(dateTimeNow)
        pointsFrameDestructing.coalesce(1).write.parquet(localSettings.dataDirectory + instant)

      }
    })

  }

}
