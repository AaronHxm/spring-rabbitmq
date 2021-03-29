package org.hxm.class1.exchange.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author : Aaron
 *
 * create at:  2021/3/22  13:50
 *
 * description: topic 生产者
 */
public class TopicProducer {

  public static void main(String[] args) throws IOException, TimeoutException {

    //交换机名称
    final String EXCHANGE_NAME = "first-topic";

    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setHost("127.0.0.1");
    //建立链接
    Connection connection = factory.newConnection();
    // 创建信道
    Channel channel = connection.createChannel();
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC, true);
    //定义消息的路由健
    String[] routerKeys = {"warning", "info", "debug"};

    String[] models= {"user", "email", "model"};

    String[] services = {"A", "B", "C"};


    for (int i = 0; i < 3; i++) {
      for(String model : models){
        for(String server : services){
          //roterKey 类似于 warning.user.A
          String routerKey = routerKeys[i]+"."+model+"."+server;
          String msg = "hello,rabbit mq ,my log level is " + routerKey;
          System.out.println(msg);
          //发送消息
          channel.basicPublish(EXCHANGE_NAME, routerKey, null, msg.getBytes());
        }
      }
    }
    channel.close();
    connection.close();


  }
}

