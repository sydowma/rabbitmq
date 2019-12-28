package com.magaofei.rabbitmq.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class RabbitTopicProducer {

    private static final String EXCHANGE_NAME = "exchange_demo";
    private static final String ROUTING_KEY = "routingkey_demo2";
    private static final String QUEUE_NAME = "queue_demo";
    private static final String IP_ADDRESS = "127.0.0.1";
    private static final int PORT = 5672;

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(IP_ADDRESS);
        connectionFactory.setPort(PORT);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "topic", true, false, null);

        for (int i = 0; i< 10000; i++) {
            String queueName = QUEUE_NAME + ":" + i;
            String routeKey = String.valueOf(i);
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, EXCHANGE_NAME, routeKey);
            String message = "Hello World" + i;
            channel.basicPublish(EXCHANGE_NAME, routeKey, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());

        }


        channel.close();
        connection.close();

        System.out.println("time = " + (System.currentTimeMillis() - startTime));
    }
}
