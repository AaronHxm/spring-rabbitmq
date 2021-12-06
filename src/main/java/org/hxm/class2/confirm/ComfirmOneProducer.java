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
public class ComfirmOneProducer {
  public static final String EXANGE_NAME = "first-confirm";


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
    channel.exchangeDeclare(ComfirmOneProducer.EXANGE_NAME, BuiltinExchangeType.DIRECT);
    //定义消息的路由健
    String routerKeys = "";
    channel.confirmSelect();
    for (int i = 0; i < 3; i++) {
      String routerKey =  ComfirmOneProducer.ROUTE_KEY;
      String msg = "hello,rabbit mq ,my log level is " + routerKey;
      System.out.println(msg);
      //发送消息
      channel.basicPublish(ComfirmOneProducer.EXANGE_NAME, routerKey, null, msg.getBytes());
      boolean b = channel.waitForConfirms();
      if(b){
        System.out.println("send success");
      }else{
        System.out.println("send error");
      }
    }
    channel.close();
    connection.close();
  }
}

