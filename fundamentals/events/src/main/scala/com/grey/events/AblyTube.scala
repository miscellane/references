package com.grey.events

import io.ably.lib.realtime.Channel.MessageListener
import io.ably.lib.realtime.{AblyRealtime, Channel}
import io.ably.lib.types.Message

class AblyTube {

  private val keyReader = new KeyReader().keyReader(apiName = "ably")

  def ablyTube(): Unit = {

    val ablyRealtime: AblyRealtime = new AblyRealtime(keyReader)

    // [product:ably-tfl/tube]tube:northern:940GZZLUEUS:arrivals
    val channel: Channel = ablyRealtime.channels.get("[product:ably-tfl/tube]tube:disruptions")

    val message: Message = new Message()

    channel.subscribe(new MessageListener {
      override def onMessage(message: Message): Unit = {
        println(message.data)
      }
    })


  }

}
