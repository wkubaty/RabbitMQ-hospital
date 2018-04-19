import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Doctor {
    private final String EXAMINATIONS_EXCHANGE = "examinations";
    private final String INFO_EXCHANGE = "info";
    private  String resultQueue;
    private String infoQueue;
    private int id;

    public Doctor(int id) {
        this.id = id;
        this.resultQueue = "result_doctor_" + id;
        this.infoQueue = "info_doctor_" + id;
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
        channel.exchangeDeclare(EXAMINATIONS_EXCHANGE, BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare(INFO_EXCHANGE, BuiltinExchangeType.DIRECT);

        // queue & bind
        channel.queueDeclare(resultQueue, false, false, false, null);
        String resultKey = String.format("%d.*.result", id);
        channel.queueBind(resultQueue, EXAMINATIONS_EXCHANGE, resultKey);
        channel.queueDeclare(infoQueue, false, false, false, null);
        channel.queueBind(infoQueue, INFO_EXCHANGE, "info");

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
        channel.basicConsume(resultQueue, true, consumer);
        channel.basicConsume(infoQueue, true, consumer);

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
                channel.basicPublish(EXAMINATIONS_EXCHANGE, String.format("%d.%s", id, type), null, message.getBytes("UTF-8"));

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
