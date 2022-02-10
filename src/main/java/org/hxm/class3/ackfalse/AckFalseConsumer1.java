package org.hxm.class3.ackfalse;

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
 *
 *
 * 消费者手动确认模式
 *
 *
 * 当生产者发送10条消息的时候，两个消费者都可以收到消息，但并没有进行确认，关闭其中一条消费者，会发现未确认的消息 会发送到另外一个消费者
  如果队列中的消息发送到消费者后，消费者不对消息进行确认，那么消息会一直留在队列中，直到确认才会删除。
  如果发送到A消费者的消息一直不确认，只有等到A消费者与rabbitmq的连接中断，rabbitmq才会考虑将A消费者未确认的消息重新投递给另一个消费者
 * 1、启动两个消费者
 * 2、启动生产者
 * 3、关闭其中一个消费者
 *
 *
 */
public class AckFalseConsumer1 {

  public static void main(String[] args) throws IOException, TimeoutException {
    final String EXCHANGE_NAME = "first-ack-false";

    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setHost("127.0.0.1");
    //建立链接
    Connection connection = factory.newConnection();
    // 创建信道
    final Channel channel = connection.createChannel();

     channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, false);
    //声明队列
    String queueName =  "consumerInfo";  //  channel.queueDeclare().getQueue();//随即名称
    //声明消费的key info
    String routingKey = "test";
//
    channel.queueDeclare(queueName,
        false,//是否持久化
        false,//是否独享
        false,//是否自动删除
        null
    );
    //绑定队列，通过键 info 将队列和交换器绑定起来
    channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
    //定义一个消费者
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
        String routingKey = envelope.getRoutingKey();
        String contentType = properties.getContentType();


        long deliveryTag = envelope.getDeliveryTag();
        //确认消息
     //   channel.basicAck(deliveryTag, false);


        String bodyStr = new String(body, "UTF-8");
        System.out.println(bodyStr);
        //  super.handleDelivery(consumerTag, envelope, properties, body);
      }
    };
    channel.basicConsume(queueName,false,consumer);
  }

}

