// Databricks notebook source
// MAGIC %md #Azure IoT Hub to Azure SQL Database - Structured Streaming with Windowing
// MAGIC 
// MAGIC This notebook contains a complete example how to read streaming data from an Azure IoT Hub with Spark Streaming in Databricks.
// MAGIC 
// MAGIC The data is aggregated with a simple windowing function and the results are written to an Azure SQL Database.
// MAGIC 
// MAGIC _Disclaimer: Although this sample works so far, it does not contain any kind of error handling and was not really tested for performance. It should merly serve as a starting point for any further solution_

// COMMAND ----------

// MAGIC %md Set up Event Hub parameters (i.e. the connection parameters for the built-in EH-compatible endpoint of IoT Hub)

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.eventhubs._
import org.apache.spark.eventhubs.ConnectionStringBuilder

val iotHubName = "XXXXXX"
val iotHubEvenHubEndpointConnectionString = "Endpoint=sb://iothub-ns-XXXXXX.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=XXXXXXXXXXXXXXXXX"
val consumerGroupName = "XXXXXXXXXX"

// Build connection string with the above information 
val connectionString = ConnectionStringBuilder(iotHubEvenHubEndpointConnectionString)
  .setEventHubName(iotHubName)
  .build

val ehConf = EventHubsConf(connectionString)
  .setConsumerGroup(consumerGroupName)

val sparkSession = SparkSession.builder().getOrCreate()
val inputStream = sparkSession
  .readStream
  .format("eventhubs")
  .options(ehConf.toMap)
  .load()

// COMMAND ----------

// MAGIC %md ####Create a JDBC writer class in a packaged cell 
// MAGIC 
// MAGIC This writer is later used to insert rows from our result dataset into an Azure SQL Database. Note that currently this uses a hard-coded INSERT statement :/
// MAGIC 
// MAGIC Important: Whenever this packaged cell is modified (after initial creation), the cluster needs to be restarted in order for those changes to take effect. Otherwise you can also change the name of the class (and again where it is being used later)

// COMMAND ----------

package com.microsoft.example

class JDBCSinkAvgTemperatur(url: String, user:String, pwd:String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row]{
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    var connection:java.sql.Connection = _
    var statement:java.sql.Statement = _

    def open(partitionId: Long, version: Long):Boolean = {
        Class.forName(driver)
        connection = java.sql.DriverManager.getConnection(url, user, pwd)
        statement = connection.createStatement
        true
    }

    def process(value: org.apache.spark.sql.Row): Unit = {    

    statement.executeUpdate("INSERT INTO dbo.DatabricksIoTAvgTemperatur(deviceid, window_start, window_end, temperatur_avg, humidity_avg) " +
                             "VALUES ('" + value(0) + "','" + value(1) + "','" + value(2) + "','" + value(3) + "','" + value(4) + "');")
    }

    def close(errorOrNull:Throwable):Unit = {
        connection.close
    }
}

// COMMAND ----------

// MAGIC %md ####Create JSON schema
// MAGIC 
// MAGIC We are not inferring the schema here. a) we know in this case what the data looks like and b) this notebook should be funny self-contained in the end.
// MAGIC 
// MAGIC Expected schema: 
// MAGIC 
// MAGIC {
// MAGIC   "deviceId": "device1",
// MAGIC   "deviceTime": "2018-02-12T13:51:42",
// MAGIC   "humidity": 89.2,
// MAGIC   "temperatur": 21.2342
// MAGIC }

// COMMAND ----------

import org.apache.spark.sql.types._

val iotTelemetryJsonSchema = (new StructType)
                                .add("deviceId", StringType)
                                .add("deviceTime", StringType)
                                .add("humidity", DoubleType)
                                .add("temperatur", DoubleType)

// COMMAND ----------

// MAGIC %md Get the body (payload) from the IoT Hub message and build the dataframe based on the JSON schema defined above. Cast to string works as in this case we know the telemetry body is in fact a string

// COMMAND ----------

import org.apache.spark.sql.functions._

val iotHubBodyDF = inputStream.select($"body".cast("string").as("value") ) // select only the body (payload) field. We are here not interested in the IoT Hub metadata etc.
val jsonIotTelemetry = iotHubBodyDF.select( from_json($"value", iotTelemetryJsonSchema).as("json"))

// COMMAND ----------

// MAGIC %md ####Filter out any events which have no deviceId set
// MAGIC 
// MAGIC Select relevant columns, alias them and also cast deviceTime field to proper Timestamp type

// COMMAND ----------

val deviceData = jsonIotTelemetry
                    .filter($"json.deviceId" =!= "") // filter out any rows without a deviceId
                    .select($"json.deviceId".as("deviceId"), 
                              $"json.temperatur".as("temperatur"), 
                              $"json.humidity".as("humidity"),
                              unix_timestamp($"json.deviceTime", "yyyy-MM-dd'T'HH:mm:ss").cast(TimestampType).as("deviceTime") // cast to TimestampType for windowing later
                  )

// COMMAND ----------

// MAGIC %md ####Windowed query
// MAGIC 
// MAGIC Group data into 10 seconds time slices and calculate the average temperatur and humidity values (per deviceId).

// COMMAND ----------

val windowedData = deviceData
      .withWatermark("deviceTime", "10 seconds")
      .groupBy($"deviceId", window($"deviceTime","10 seconds", "10 seconds"))
      .agg(avg("temperatur").as("temperatur_avg"), avg("humidity").as("humidity_avg"))

val windowedDataSelected = windowedData.select($"deviceId", 
                                               $"window.start", 
                                               $"window.end", 
                                               $"temperatur_avg", 
                                               $"humidity_avg")

// COMMAND ----------

// MAGIC %md Create SQL Database connection and reference the writer class defined above

// COMMAND ----------

import com.microsoft.example

val jdbcUsername = "XXXXXXXXX"
val jdbcPassword = "XXXXXXXXXXXXXX"
val jdbcHostname = "XXXXXXXXXXXXXXX.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "XXXXXXXX"

val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
val writer = new com.microsoft.example.JDBCSinkAvgTemperatur(jdbcUrl, jdbcUsername, jdbcPassword)

// COMMAND ----------

// MAGIC %md ####Run the actual streaming job
// MAGIC 
// MAGIC This cell will keep running until it is stopped (or runs into an error)

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val windowedQuery = windowedDataSelected
  .writeStream
  .option("checkpointLocation", "/Ehub/eventhub-checkpoint_windowed_query")
  .foreach(writer)
  .outputMode("append")
  .trigger(Trigger.ProcessingTime("10 seconds"))  
  .start()
