package org.hxm.class1.exchange.direct;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author : Aaron
 *
 * create at:  2021/3/10  21:35
 *
 * description: direct 类型 消费某一个key
 */
public class NormalConsumer {


  public static void main(String[] args) throws IOException, TimeoutException {
    final String EXCHANGE_NAME = "first-direct";

    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setHost("127.0.0.1");
    //建立链接
    Connection connection = factory.newConnection();
    // 创建信道
    final Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true);
    //声明队列
    String queueName =  "consumerInfo";  //  channel.queueDeclare().getQueue();//随即名称
    //声明消费的key info
    String routingKey = "debug";
//
    channel.queueDeclare(queueName,
        false,//是否持久化
        false,//是否独享
        false,//是否自动删除
        null
    );
    //绑定队列，通过键 info 将队列和交换器绑定起来
    channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
    System.out.println("消费者已运行，正在等待消息");
    //定义一个消费者
    final Consumer consumer = new DefaultConsumer(channel) {
      /**
       * No-op implementation of {@link Consumer#handleDelivery}.
       */
      //重写获取消息方法
      @Override
      public void handleDelivery(String consumerTag,
          Envelope envelope,
          BasicProperties properties,
          byte[] body) throws IOException {
        String routingKey = envelope.getRoutingKey();
        String contentType = properties.getContentType();
        System.out.println("消费的路由键：" + routingKey);
        System.out.println("消费的内容类型：" + contentType);
        long deliveryTag = envelope.getDeliveryTag();
        //确认消息
        channel.basicAck(deliveryTag, false);
        System.out.println("消费的消息体内容：");
        String bodyStr = new String(body, "UTF-8");
        System.out.println(bodyStr);
        //  super.handleDelivery(consumerTag, envelope, properties, body);
      }
    };
    channel.basicConsume(queueName,true,consumer);
  }
}

