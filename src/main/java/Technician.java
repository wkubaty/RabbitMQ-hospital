import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Technician {
    private final String EXCHANGE_NAME = "examinations";
    private int id;
    private Set<BodyPart> examinationSkills;
    private Technician(int id, Set<BodyPart> examinationSkills){
        this.id = id;
        this.examinationSkills = examinationSkills;
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
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        // queue & bind
        examinationSkills
                .forEach(bodyPart -> {
                        try {
                            String queueName = bodyPart.name();
                            channel.queueDeclare(queueName, false, false, false, null);
                            channel.queueBind(queueName, EXCHANGE_NAME, String.format("*.%s", bodyPart.name()));
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
                    channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        // start listening
        System.out.println("Waiting for messages...");
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
        s.add(BodyPart.HIP);
        s.add(BodyPart.KNEE);
        Technician technician = new Technician(1, s);
        technician.work();
    }
}
