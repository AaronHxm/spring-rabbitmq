package cn.enjoyedu.transaction;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**

 *类说明：存放到延迟队列的元素，对业务数据进行了包装
 */
public class ProducerTransaction {

    public final static String EXCHANGE_NAME = "producer_transaction";

    public static void main(String[] args)
            throws IOException, TimeoutException, InterruptedException {
        /**
         * 创建连接连接到RabbitMQ
         */
        ConnectionFactory factory = new ConnectionFactory();

        // 设置MabbitMQ所在主机ip或者主机名
        factory.setHost("127.0.0.1");
        // 创建一个连接
        Connection connection = factory.newConnection();
        // 创建一个信道
        Channel channel = connection.createChannel();
        // 指定转发
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        String[] severities={"error","info","warning"};
        channel.txSelect();
        try {
            for(int i=0;i<3;i++){
                String severity = severities[i%3];
                // 发送的消息
                String message = "Hello World_"+(i+1)
                        +("_"+System.currentTimeMillis());
                channel.basicPublish(EXCHANGE_NAME, severity, true,
                        null, message.getBytes());
                System.out.println("----------------------------------");
                System.out.println(" Sent Message: [" + severity +"]:'"
                        + message + "'");
                Thread.sleep(200);
            }
            channel.txCommit();
        } catch (IOException e) {
            e.printStackTrace();
            channel.txRollback();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 关闭频道和连接
        channel.close();
        connection.close();
    }


}
