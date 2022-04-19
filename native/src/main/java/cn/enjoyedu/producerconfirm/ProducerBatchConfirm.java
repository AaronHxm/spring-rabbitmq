package cn.enjoyedu.producerconfirm;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**

 *类说明：
 */
public class ProducerBatchConfirm {

    public final static String EXCHANGE_NAME = "producer_wait_confirm";
    private final static String ROUTE_KEY = "error";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        /**
         * 创建连接连接到RabbitMQ
         */
        ConnectionFactory factory = new ConnectionFactory();
        // 设置RabbitMQ所在主机ip或者主机名
        factory.setHost("127.0.0.1");
        // 创建一个连接
        Connection connection = factory.newConnection();
        // 创建一个信道
        Channel channel = connection.createChannel();
        // 指定转发
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        channel.addReturnListener(new ReturnListener() {
            public void handleReturn(int replyCode, String replyText,
                                     String exchange, String routingKey,
                                     AMQP.BasicProperties properties,
                                     byte[] body)
                    throws IOException {
                String message = new String(body);
                System.out.println("RabbitMq返回的replyCode:  "+replyCode);
                System.out.println("RabbitMq返回的replyText:  "+replyText);
                System.out.println("RabbitMq返回的exchange:  "+exchange);
                System.out.println("RabbitMq返回的routingKey:  "+routingKey);
                System.out.println("RabbitMq返回的message:  "+message);
            }
        });
        // 启用发送者确认模式
        channel.confirmSelect();

        //所有日志严重性级别
        for(int i=0;i<2;i++){
            // 发送的消息
            String message = "Hello World_"+(i+1);
            //参数1：exchange name
            //参数2：routing key
            channel.basicPublish(EXCHANGE_NAME, ROUTE_KEY, true,null, message.getBytes());
            System.out.println(" Sent Message: [" + ROUTE_KEY +"]:'"+ message + "'");
        }
        channel.waitForConfirmsOrDie();
        // 关闭频道和连接
        channel.close();
        connection.close();
    }

}
