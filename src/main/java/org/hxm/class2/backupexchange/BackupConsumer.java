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
 * create at:  2021/12/27  13:19
 *
 * description:
 */
public class BackupConsumer {
  public static final String EXCHANGE_NAME = "backup";

  public static void main(String[] args) throws Exception {
    //创建连接,连接到RabbitMQ,与发送端一样
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost("127.0.0.1");
    Connection connection = connectionFactory.newConnection();

    //创建信道
    Channel channel = connection.createChannel();
    //绑定交换器
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT,true,false,null);

    //创建一个随机队列
    String queueName = channel.queueDeclare().getQueue();

    //将队列和交换器通过路由键进行绑定
    //备用交换器中，我们一般定义为FANOUT模式，然后定义了一个路由键#，让其消费者来接受这些无法路由的消息进行相关的处理
    channel.queueBind(queueName, EXCHANGE_NAME, "#");

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

