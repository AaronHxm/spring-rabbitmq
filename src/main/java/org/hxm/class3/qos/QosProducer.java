package org.hxm.class3.qos;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author : Aaron
 *
 * create at:  2022/2/10  14:40
 *
 * description: 手动确认生产者
 */
public class QosProducer {
  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

    //交换机名称
    final String EXCHANGE_NAME = "first-qos";

    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setHost("127.0.0.1");
    //建立链接
    Connection connection = factory.newConnection();
    // 创建信道
    Channel channel = connection.createChannel();
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, false);


    //定义消息的路由健 未被使用过的路由键
    String routerKey = "msg";


    //发送消息
    for (int i =0;i<210;i++){
      String msg = "hello,rabbit mq ,my log level is " + routerKey+i;
      if(i==209){
        msg = "stop";
      }

      System.out.println(msg);
      channel.basicPublish(EXCHANGE_NAME, routerKey, false, null, msg.getBytes());

    }

    channel.close();
    connection.close();
  }
}

