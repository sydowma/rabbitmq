package com.magaofei.rabbitmq.topic;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RabbitTopicConsumer {

    private static final String EXCHANGE_NAME = "exchange_demo";
    private static final String ROUTING_KEY = "routingkey_demo2";
    private static final String QUEUE_NAME = "queue_demo";

    private static final String IP_ADDRESS = "127.0.0.1";
    private static final int PORT = 5672;

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

        long startTime = System.currentTimeMillis();
        Address[] addresses = new Address[] {
                new Address(IP_ADDRESS, PORT)
        };
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection(addresses);
        final Channel channel = connection.createChannel();

        for (int i = 0; i < 10000; i++) {
            String queueName = QUEUE_NAME + ":" + i;
            String routeKey = String.valueOf(i);
            channel.exchangeDeclare(EXCHANGE_NAME, "topic", true, false, null);
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, EXCHANGE_NAME, routeKey);

            channel.basicQos(64);
            Consumer consumer = new DefaultConsumer(channel) {

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
//                super.handleDelivery(consumerTag, envelope, properties, body);
                    System.out.println("message = " + new String(body));
//                    try {
//                        TimeUnit.SECONDS.sleep(1);
//                    } catch (InterruptedException e) {
//                        Thread.currentThread().interrupt();
//                    }
                    channel.basicAck(envelope.getDeliveryTag(), false);

//                    System.out.println("basicReject");
//                    channel.basicReject(envelope.getDeliveryTag(), true);
                }
            };
            channel.basicConsume(queueName, consumer);
//            System.out.println("consume ");
        }
        channel.close();
        connection.close();

        System.out.println("time = " + (System.currentTimeMillis() - startTime));

    }
}
