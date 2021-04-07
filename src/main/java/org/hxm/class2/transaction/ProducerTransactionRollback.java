package org.hxm.class2.transaction;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author aaron.hu
 * @Description: 事务回滚
 * @date 2021/4/7 16:53
 */
public class ProducerTransactionRollback {

    public static final String EXCHANGE_NAME = "first-transaction";

    public static final String ROUTER_KEY = "transaction";

    public static void main(String[] args) throws IOException, TimeoutException {
        //交换机名称
        final String EXCHANGE_NAME = ProducerTransaction.EXCHANGE_NAME;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setHost("127.0.0.1");
        //建立链接
        Connection connection = factory.newConnection();
        // 创建信道
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true);

        //定义消息的路由健 未被使用过的路由键
        String routerKey = "transaction";
        String msg = "hello,rabbit mq ,my log level is " + routerKey;
        System.out.println(msg);
        //开启事务
        try {
            channel.txSelect();
            //发送消息
            channel.basicPublish(EXCHANGE_NAME, ROUTER_KEY, null, msg.getBytes());
            int result = 1/0;
            //提交事务
            channel.txCommit();
        }catch (Exception e){
            channel.txRollback();
        }

        channel.close();
        connection.close();

    }
}
