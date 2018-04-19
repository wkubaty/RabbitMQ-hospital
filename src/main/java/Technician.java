import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Technician {
    private final String LISTENING_QUEUE = "listen";
    private final String EXAMINATIONS_EXCHANGE = "examinations";
    private final String INFO_EXCHANGE = "info";
    private String infoQueue;
    private int id;
    private Set<BodyPart> examinationSkills;
    private Technician(int id, Set<BodyPart> examinationSkills){
        this.id = id;
        this.examinationSkills = examinationSkills;
        this.infoQueue = "info_technician_" + id;
    }

    private void work() throws Exception{
        // info

        System.out.println("Technician started...");

        // connection & channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        // exchange
        channel.exchangeDeclare(EXAMINATIONS_EXCHANGE, BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare(INFO_EXCHANGE, BuiltinExchangeType.DIRECT);
        channel.queueDeclare(infoQueue, false, false, false, null);
        channel.queueBind(infoQueue, INFO_EXCHANGE, "info");
        System.out.println("created queue: " + LISTENING_QUEUE);
        // queue & bind
        examinationSkills
                .forEach(bodyPart -> {
                        try {
                            String queueName = bodyPart.name();
                            channel.queueDeclare(queueName, false, false, false, null);
                            channel.queueBind(queueName, EXAMINATIONS_EXCHANGE, String.format("*.%s", bodyPart.name()));
                            System.out.println("created queue: " + bodyPart.name());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });

        // consumer (message handling)
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received: " + message);
                if(envelope.getExchange().equals(EXAMINATIONS_EXCHANGE)){
                    try {
                        Random rand = new Random();
                        int sec = rand.nextInt(10) + 1;
                        System.out.print(String.format("Examining (~%d s)...", sec));
                        Thread.sleep( sec * 1000);
                        System.out.println("done");
                        String routingKey = envelope.getRoutingKey(); //doctor id
                        message += " done";
                        routingKey += ".result";
                        System.out.println("Sending result with key: " + routingKey);
                        channel.basicPublish(EXAMINATIONS_EXCHANGE, routingKey, null, message.getBytes("UTF-8"));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        };

        // start listening
        System.out.println("Waiting for messages...");
        channel.basicQos(1);
        channel.basicConsume(infoQueue, true, consumer);

        examinationSkills
                .forEach(bodyPart -> {
                    try {
                        String queueName = bodyPart.name();
                        channel.basicConsume(queueName, true, consumer);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    public static void main(String[] args) throws Exception{
        HashSet<BodyPart> s = new HashSet<>();
        s.add(BodyPart.ELBOW);
        s.add(BodyPart.KNEE);
        Technician technician = new Technician(2, s);
        technician.work();
    }
}
