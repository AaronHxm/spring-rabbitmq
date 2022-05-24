package org.hxm.class3.rejectMsg;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : Aaron
 *
 * create at:  2022/2/11  10:03
 *
 * description: 消息拒绝的生产者
 *
 * 消息拒绝有如下两种api
 *  * 1. channel.basicReject(envelope.getDeliveryTag(), false);
 *  * 2. channel.basicNack(envelope.getDeliveryTag(), false, false);
 *  * 这2种方式的区别在于basicNack有一个批量拒绝的功能
 *  * requeue为true时，消息会重新投放给任意一个消费者（包括拒绝消息的那个消费者）
 */
@Slf4j
public class RejectMsgProducer {
  public static final String EXCHANGE_NAME = "rejectMsg_exchange";

  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();


    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

    String routingKey = "error";
    for (int i = 0; i < 10; i++) {
      String message = "hello rabbitmq " + i;
      channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
     // log.info("send message, routingKey: {}, message: {}", routingKey ,message);

      System.out.println(String.format("send message, routingKey: %s, message: %s", routingKey ,message));
    }
    channel.close();
    connection.close();
  }
}

