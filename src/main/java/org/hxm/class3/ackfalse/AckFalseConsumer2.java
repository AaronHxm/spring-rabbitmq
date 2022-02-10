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
 *
 */
public class AckFalseConsumer2 {

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
    String routingKey = "msg";
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
        /**
         * 实际开发中应处理完成后在确认消息
         */
        try {
          String routingKey = envelope.getRoutingKey();
          String contentType = properties.getContentType();

          long deliveryTag = envelope.getDeliveryTag();
          //确认消息

          System.out.println("----------------");
          String bodyStr = new String(body, "UTF-8");
          System.out.println(bodyStr);
          channel.basicAck(deliveryTag, false);
        }catch (Exception e){
      //出现异常  不确认


        }



        //
      }
    };
    channel.basicConsume(queueName,false,consumer);
  }

}

