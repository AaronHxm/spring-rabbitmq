package org.hxm.class3.rejectMsg;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.hxm.class3.rejectMsg.RejectMsgProducer;

/**
 * @author : Aaron
 *
 * create at:  2022/2/11  10:24
 *
 * description:
 */
@Slf4j
public class NormalConsumer {


  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();


    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(RejectMsgProducer.EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

    String queueName = "errorQueue";
    channel.queueDeclare(queueName, false, false, false, null);

    String bindingKey = "error";
    channel.queueBind(queueName, RejectMsgProducer.EXCHANGE_NAME, bindingKey);

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
          AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        channel.basicAck(envelope.getDeliveryTag(), false);
        System.out.println(String.format("get message, routingKey: %s, message: %s", envelope.getRoutingKey() ,message));
      }
    };

    channel.basicConsume(queueName, false, consumer);
  }
}

