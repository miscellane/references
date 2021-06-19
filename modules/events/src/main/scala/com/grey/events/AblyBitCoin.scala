package com.grey.events

import io.ably.lib.realtime.{AblyRealtime, Channel}
import io.ably.lib.realtime.Channel.MessageListener
import io.ably.lib.types.Message

class AblyBitCoin {

  private val keyReader = new KeyReader().keyReader(apiName = "ably")

  def ablyBitCoin(): Unit = {

    val ablyRealtime: AblyRealtime = new AblyRealtime(keyReader)

    val channel: Channel = ablyRealtime.channels.get("[product:ably-bitflyer/bitcoin]" + "bitcoin:jpy")

    val message: Message = new Message()

    channel.subscribe(new MessageListener {
      override def onMessage(message: Message): Unit = {
        println(message.data)
      }
    })

  }

}
