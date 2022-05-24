package org.hxm.class3.resreq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * @author : Aaron
 *
 * create at:  2021/3/22  13:17
 *
 * description: res-req 生产者
 */
public class Produer {

  public static void main(String[] args) throws IOException, TimeoutException {
    //交换机名称
    final String EXCHANGE_NAME = "first-req-resp";

    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setHost("127.0.0.1");
    //建立链接
    Connection connection = factory.newConnection();
    // 创建信道
    Channel channel = connection.createChannel();
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true);
    //定义消息的路由健
    String uuid = UUID.randomUUID().toString();

    String reponseQueue = channel.queueDeclare().getQueue();
    AMQP.BasicProperties basicProperties = new AMQP.BasicProperties()
        .builder()
        .messageId(uuid)
        .replyTo(reponseQueue).build();
    final com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel) {
      /**
       * No-op implementation of {@link com.rabbitmq.client.Consumer#handleDelivery}.
       */
      //重写获取消息方法
      @Override
      public void handleDelivery(String consumerTag,
          Envelope envelope,
          BasicProperties properties,
          byte[] body) throws IOException {
        String bodyStr = new String(body, "UTF-8");
        System.out.println("客户端的回应：" + bodyStr);

      }
    };
    //消费者消费生产者的回答

    channel.basicConsume(reponseQueue, true, consumer);

    String msg = "hello,消费者 ，我是生产者 ";
    System.out.println(msg);
    //发送消息
    channel.basicPublish(EXCHANGE_NAME, "error", basicProperties, msg.getBytes());

//    channel.close();
//    connection.close();
  }
}

