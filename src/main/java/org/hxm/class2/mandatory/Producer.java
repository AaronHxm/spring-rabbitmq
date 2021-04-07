package org.hxm.class2.mandatory;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author aaron.hu
 * @Description:
 * @date 2021/4/7 9:29
 */
public class Producer {

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

        //交换机名称
        final String EXCHANGE_NAME = "first-mandatory";

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setHost("127.0.0.1");
        //建立链接
        Connection connection = factory.newConnection();
        // 创建信道
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true);

        channel.addReturnListener(new ReturnListener() {
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("replyText: " + replyText);
                System.out.println("exchange: " + exchange);
                System.out.println("routingKey: " + routingKey);
                System.out.println("message: " + new String(body));
            }
        });
        /**
         * 通道关闭时通知
         */
        channel.addShutdownListener(new ShutdownListener() {
            public void shutdownCompleted(ShutdownSignalException cause) {
                System.out.println(cause.getMessage());

            }
        });

        //定义消息的路由健 未被使用过的路由键
        String routerKey = "mandatory";
        String msg = "hello,rabbit mq ,my log level is " + routerKey;
        System.out.println(msg);
        //发送消息
        channel.basicPublish(EXCHANGE_NAME, routerKey, true, null, msg.getBytes());
        Thread.sleep(2000L);
        channel.close();
        connection.close();
    }
}
