package org.hxm.class2.backupexchange;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;

/**
 * @author : Aaron
 *
 * create at:  2021/12/27  13:18
 *
 * description: 发送error 信息
 */
public class ErrorConsumer {
  public static final String EXCHANGE_NAME = "logs";

  public static void main(String[] args) throws Exception {
    //创建连接,连接到RabbitMQ,与发送端一样
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost("127.0.0.1");
    Connection connection = connectionFactory.newConnection();


    //创建信道
    Channel channel = connection.createChannel();
    //可不创建，由生产者进行创建
    //channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, false, false, null);

    String queueName = "logError";  //声明一个队列名称
    String routingKey = "error";    //路由键名称

    //创建一个队列
    channel.queueDeclare(queueName, false, false, false, null);

   // channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT,false,false,argsMap);

    //将队列和交换器通过路由键进行绑定
    channel.queueBind(queueName, EXCHANGE_NAME, routingKey);

    //声明了一个消费者
    Consumer consumer = new DefaultConsumer(channel){
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = "Exchange: " + envelope.getExchange() + ", " +
            "RoutingKey: " + envelope.getRoutingKey() + ", " +
            "Content: " + new String(body, "UTF-8");
        System.out.println(message);
      }
    };
    //消费者正式开始在指定队列上消费消息
    channel.basicConsume(queueName, true, consumer);
  }
}

