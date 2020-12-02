import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Application {

    public static void main(String[] args) {
        //*****************
        // YOUR CODE HERE
        //*****************
    }

    public static void consumeMessages(String topic, Consumer<String, Transaction> kafkaConsumer) {
        //*****************
        // YOUR CODE HERE
        //*****************
    }

//    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
    //*****************
    // YOUR CODE HERE
    //*****************
//    }

    private static void approveTransaction(Transaction transaction) {
        // Print transaction information to the console

        //*****************
        // YOUR CODE HERE
        //*****************
    }

}
