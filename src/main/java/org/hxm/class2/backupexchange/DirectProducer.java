package org.hxm.class2.backupexchange;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * @author : Aaron
 *
 * create at:  2021/12/24  09:53
 *
 * description:
 */
public class DirectProducer {
  //交换器名称
  public static final String EXCHANGE_NAME = "logs";
  public static final String BACKUP_EXCHANGE_NAME = "backup";

  public static void main(String[] args) throws Exception {
    //创建连接,连接到RabbitMQ
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost("127.0.0.1");
    Connection connection = connectionFactory.newConnection();

    //创建信道
    Channel channel = connection.createChannel();

    //声明备用交换器
    Map<String,Object> argsMap = new HashMap<>();
    argsMap.put("alternate-exchange",BACKUP_EXCHANGE_NAME);

    //创建主交换器
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT,false,false,argsMap);
    //创建备用交换器 备用交换器一般都是设置FANOUT模式
    channel.exchangeDeclare(BACKUP_EXCHANGE_NAME, BuiltinExchangeType.FANOUT, true, false, null);

    //定义的业务日志消息级别，即作为路由键使用
    String[] logLevels = {"error", "warn", "info"};
    for (int i = 0; i < logLevels.length; i++) {
      String logLevel = logLevels[i];
      String msg = "Hello RabbitMQ "+logLevel;
      System.out.println(msg);
      //发布消息，需要参数：交换器、路由键，其中以日志消息级别为路由键
      channel.basicPublish(EXCHANGE_NAME, logLevel, null, msg.getBytes(Charset.forName("UTF-8")));
    }
    Thread.sleep(2000);
    channel.close();
    connection.close();
  }
}

