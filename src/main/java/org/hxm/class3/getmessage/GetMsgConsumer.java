package org.hxm.class3.getmessage;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author aaron.hu
 * @Description: get/pull 模式的消费者
 * @date 2021/4/7 9:51
 */
@SuppressWarnings("ALL")
public class GetMsgConsumer {

  public static void main(String[] args) throws IOException, TimeoutException {
    final String EXCHANGE_NAME = "first-getMsg";

    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setHost("127.0.0.1");
    //建立链接
    Connection connection = factory.newConnection();
    // 创建信道
    final Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
    //声明队列
    String queueName = "getMsg";  //  channel.queueDeclare().getQueue();//随即名称
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
    System.out.println("等待接收消息-------------");

    //每次只能拿到一条消息 如果队列中没有消息 则 拿到的GetResponse为null
      //好处是 消费者可以控制多久去拿一次消息
      while (true){
          GetResponse getResponse = channel.basicGet(queueName, true);

          if (null != getResponse) {
              System.out.println("消费的路由键：" + getResponse.getEnvelope().getRoutingKey());
              String string = new String(getResponse.getBody(), "UTF-8");
              System.out.println("消费的内容类型：" + string);
          }
      }


  }
}
