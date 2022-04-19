/**
 * 类说明：死信交换器
 * RabbitMQ对AMQP规范的一个扩展。被投递消息被拒绝后的一个可选行为
 * 在创建队列的时候，声明该交换器将用作保存被拒绝的消息即可
 * 相关的参数是x-dead-letter-exchange
 */
package cn.enjoyedu.dlx;
