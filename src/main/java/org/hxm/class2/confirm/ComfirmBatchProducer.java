package org.hxm.class2.confirm;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author : Aaron
 *
 * create at:  2021/12/6  13:25
 *
 * description:
 */
public class ComfirmBatchProducer {

  public static final String EXANGE_NAME = "first-batch-confirm";


  public static final String ROUTE_KEY = "error";
  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {



    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setHost("127.0.0.1");
    //建立链接
    Connection connection = factory.newConnection();
    // 创建信道
    Channel channel = connection.createChannel();
    channel.exchangeDeclare(ComfirmBatchProducer.EXANGE_NAME, BuiltinExchangeType.DIRECT, true);
    //定义消息的路由健

    channel.confirmSelect();
    for (int i = 0; i < 3; i++) {
      String routerKey = ComfirmBatchProducer.ROUTE_KEY;
      String msg = "hello,rabbit mq ,my log level is " + routerKey;
      System.out.println(msg);
      //发送消息
      channel.basicPublish(ComfirmBatchProducer.EXANGE_NAME, routerKey, null, msg.getBytes());

    }
    //Todo
    channel.waitForConfirmsOrDie();
    channel.close();
    connection.close();
  }
}

