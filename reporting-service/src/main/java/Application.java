import org.apache.kafka.clients.consumer.Consumer;

import java.util.List;

public class Application {

    public static void main(String[] args) {
        //*****************
        // YOUR CODE HERE
        //*****************
    }

    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        //*****************
        // YOUR CODE HERE
        //*****************
    }

//    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        //*****************
        // YOUR CODE HERE
        //*****************
//    }

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // Print transaction information to the console
        // Print a different message depending on whether transaction is suspicious or valid

        //*****************
        // YOUR CODE HERE
        //*****************
    }

}
