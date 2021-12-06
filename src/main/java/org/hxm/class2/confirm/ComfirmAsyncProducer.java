package org.hxm.class2.confirm;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

/**
 * @author : Aaron
 *
 * create at:  2021/12/6  13:25
 *
 * description: 异步
 */
@Slf4j
 public class ComfirmAsyncProducer {

  public static final String EXANGE_NAME = "first-batch-async-confirm";


  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {



    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setHost("127.0.0.1");
    //建立链接
    Connection connection = factory.newConnection();
    // 创建信道
    final Channel channel = connection.createChannel();
    channel.exchangeDeclare(EXANGE_NAME, BuiltinExchangeType.DIRECT, true);
      //定义消息的路由健
    String[] routerKeys = {"warning", "info", "error"};
    channel.confirmSelect();
    channel.addConfirmListener(new ConfirmListener() {
      //成功返回ack
       @Override
       public void handleAck(long deliveryTag, boolean multiple) throws IOException {
         System.out.println("成功，deliveryTag:"+deliveryTag+",multiple: "+multiple);
        log.info("成功，deliveryTag:{},multiple:{}",deliveryTag,multiple);
      }
    // fa发生内部错误 无法投递到d队列时 返回nack
       @Override
       public void handleNack(long deliveryTag, boolean multiple) throws IOException {
         System.out.println("失败，deliveryTag:"+deliveryTag+",multiple: "+multiple);

      }
    });

    for (int i = 0; i < 100; i++) {
      String routerKey = routerKeys[i%3];
      String msg = "hello,rabbit mq ,my log level is " + routerKey;
     // System.out.println(msg);
      //发送消息
      channel.basicPublish(EXANGE_NAME, routerKey, null, msg.getBytes());

    }

    channel.close();
    connection.close();
  }
}

