package org.hxm.class2.confirm;

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
 * create at:  2021/12/6  10:18
 *
 * description: 消息确认消费者
 */
public class ConfirmConsumer {

  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("127.0.0.1");
    factory.setPassword("guest");
    factory.setUsername("guest");
    Connection connection = factory.newConnection();
    final Channel channel = connection.createChannel();
    channel.exchangeDeclare(ComfirmAsyncProducer.EXANGE_NAME, BuiltinExchangeType.DIRECT,true);
    String queueName = "consumerInfo";
    channel.queueBind(queueName,ComfirmAsyncProducer.EXANGE_NAME,ComfirmOneProducer.ROUTE_KEY);
    channel.queueDeclare(queueName,
        false,//是否持久化
        false,//是否独享
        false,//是否自动删除
        null
    );
    System.out.println("消费者已运行，正在等待消息");

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
//        System.out.println("消费的路由键：" + routingKey);
//        System.out.println("消费的内容类型：" + contentType);
        long deliveryTag = envelope.getDeliveryTag();
        //确认消息
        channel.basicAck(deliveryTag, false);
//        System.out.println("消费的消息体内容：");
        String bodyStr = new String(body, "UTF-8");
        System.out.println(bodyStr);
        //  super.handleDelivery(consumerTag, envelope, properties, body);
      }
    };
    channel.basicConsume(queueName,true,consumer);
  }
}

