package org.hxm.class3.getmessage;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author : Aaron
 *
 * create at:  2022/2/10  13:08
 *
 * description: 获取消息生产者
 */
public class GetMsgProducer {
  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

    //交换机名称
    final String EXCHANGE_NAME = "first-getMsg";

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
    for (int i =0;i<10;i++){
      String msg = "hello,rabbit mq ,my log level is " + routerKey+i;
      System.out.println(msg);
      channel.basicPublish(EXCHANGE_NAME, routerKey, false, null, msg.getBytes());

    }

    channel.close();
    connection.close();
  }
}

