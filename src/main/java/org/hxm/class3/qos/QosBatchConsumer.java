package org.hxm.class3.qos;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author : Aaron
 *
 * create at:  2021/3/22  13:18
 *
 * description: fanout comsumer
 */
public class QosBatchConsumer extends DefaultConsumer {

  private int MST_COUNT = 0;

  /**
   * Constructs a new instance and records its association to the passed-in channel.
   *
   * @param channel the channel to which this consumer is attached
   */
  public QosBatchConsumer(Channel channel) {
    super(channel);

    System.out.println("批量消费者启动...");
  }

  @Override
  public void handleDelivery(String consumerTag, Envelope envelope,
      AMQP.BasicProperties properties,
      byte[] body) throws IOException {
    String message = new String(body, "UTF-8");
    System.out.println("批量消费者Received[" + envelope.getRoutingKey()
        + "]" + message);
    MST_COUNT++;
    if (MST_COUNT % 50 == 0) {
      this.getChannel().basicAck(envelope.getDeliveryTag(),
          true);
      System.out.println("批量消费者进行消息的确认-------------");
    }
    if (message.equals("stop")) {
      this.getChannel().basicAck(envelope.getDeliveryTag(),
          true);
      System.out.println("批量消费者进行最后部分业务消息的确认-------------");
    }
  }
}

