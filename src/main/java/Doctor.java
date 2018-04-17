import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Doctor {
    private final String RESULT_QUEUE = "results";
    private final String EXCHANGE_NAME = "examinations";
    private int id;

    public Doctor(int id) {
        this.id = id;
    }

    private void work() throws Exception{
        // info
        System.out.println(String.format("Doctor (id=%d) started...", id));

        // connection & channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // exchange
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        // queue & bind
        channel.queueDeclare(RESULT_QUEUE, false, false, false, null);
        String resultKey = String.format("%d.*.result", id);
        channel.queueBind(RESULT_QUEUE, EXCHANGE_NAME, resultKey);

        // consumer (message handling)
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received: " + message);
            }
        };

        // start listening
        System.out.println("Waiting for results...");
        channel.basicQos(1);
        channel.basicConsume(RESULT_QUEUE, true, consumer);

        while (true) {
            // read msg
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            System.out.println("Enter patient name: ");
            String name = br.readLine();
            System.out.println("Enter body part: ");
            String type = br.readLine();
            try {
                BodyPart examinationType = Enum.valueOf(BodyPart.class, type);
                String message = name + " " + examinationType;
                // break condition
                if (message.equals("exit")) {
                    break;
                }
                // publish
                channel.basicPublish(EXCHANGE_NAME, String.format("%d.%s", id, type), null, message.getBytes("UTF-8"));

                System.out.println("Sent: " + message);
            }
            catch (IllegalArgumentException e){
                System.out.println("Wrong body part. Select one from: 'KNEE', 'HIP', 'ELBOW'");
            }
        }
    }

    public static void main(String[] args) throws Exception{
        new Doctor(2).work();
    }
}
