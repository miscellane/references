package com.grey.events

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

class TextStreaming(streamingContext: StreamingContext) {

  def textStreaming(): Unit = {

    // Create a DStream that will connect to localhost:9999 => hostname:port
    // Herein, lines is the input DStream
    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)

    // Split each line into words
    val words: DStream[String] = lines.flatMap(_.split(" "))

    // Shape, length, etc.
    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
    wordCounts.print()

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
