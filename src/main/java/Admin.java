import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Admin {
    private final String LISTENING_QUEUE = "listen";
    private final String EXAMINATIONS_EXCHANGE = "examinations";
    private final String INFO_EXCHANGE = "info";
    private int id;
    private Admin(int id){
        this.id = id;
    }

    private void work() throws Exception{
        // info
        System.out.println("Admin started...");

        // connection & channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        // exchange
        channel.exchangeDeclare(EXAMINATIONS_EXCHANGE, BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare(INFO_EXCHANGE, BuiltinExchangeType.DIRECT);

        // queue & bind
        channel.queueDeclare(LISTENING_QUEUE, false, false, false, null);
        channel.queueBind(LISTENING_QUEUE, EXAMINATIONS_EXCHANGE, "#");
        System.out.println("created queue: " + LISTENING_QUEUE);



        // consumer (message handling)
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received: " + message + " with a routing key: " + envelope.getRoutingKey());
            }
        };

        // start listening
        System.out.println("Waiting for messages...");
        channel.basicConsume(LISTENING_QUEUE, true, consumer);

        while (true) {
            // read msg
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter info message, to send to everyone: ");
            String message = br.readLine();
            if (message.equals("exit")) {
                break;
            }
            // publish
            channel.basicPublish(INFO_EXCHANGE, "info", null, message.getBytes("UTF-8"));

            System.out.println("Sent: " + message);
        }
    }

    public static void main(String[] args) throws Exception{
        Admin admin = new Admin(1);
        admin.work();
    }
}
