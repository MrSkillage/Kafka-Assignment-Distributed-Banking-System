import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

public class Application {
    // Final String List of the TRANSACTIONS_TOPICS and Servers available
    private static final List<String> TRANSACTIONS_TOPICS = Collections.unmodifiableList(
            Arrays.asList("valid-transactions","suspicious-transactions","high-value-transactions"));
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        Application kafkaReportingNotificationConsumerApp = new Application();

        String consumerGroup = "reporting-service";
        // Print out message of which Consumer Group we belong to
        System.out.println("Consumer is part of consumer group " + consumerGroup);

        // Call createKafkaConsumer method and pass the Servers and Consumer Group
        Consumer<String, Transaction> reportingConsumer = kafkaReportingNotificationConsumerApp.createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        // Call consumerMessages method and pass the TOPIC and Consumer we creates above
        kafkaReportingNotificationConsumerApp.consumeMessages(TRANSACTIONS_TOPICS, reportingConsumer);
    }

    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        // Subscribe the consumer to the topic passed
        kafkaConsumer.subscribe(topics);

        // Create and Infinite loop while continuously checking for new messages
        while (true) {
            // ConsumerRecords stores a list of ConsumerRecords and polls every 1 second
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            // If consumerRecords is not empty i.e. has a record then do
            if (!consumerRecords.isEmpty()) {
                // For each ConsumerRecord in the list ConsumerRecords print a consume message with formatted details
                for (ConsumerRecord<String, Transaction> record : consumerRecords) {
                    // Call function to print different transaction statements
                    recordTransactionForReporting(record.topic(), record.value());
                }
            } else {
                // DO NOTHING FOR NOW
            }
            // Tell kafka its done processing messages with a commit as a final confirmation
            kafkaConsumer.commitAsync();
        }

    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        // Make a new Properties object called prop
        Properties prop = new Properties();

        // Set the various Properties used to setup a Consumer Configuration
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Returns a new KafkaConsumer made with the properties we set in prop
        return new KafkaConsumer<String, Transaction>(prop);
    }

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // Print transaction information to the console
        // Print a different message depending on whether transaction is suspicious or valid
        if (topic.equals(TRANSACTIONS_TOPICS.get(0))) {
            System.out.println(String.format("Recording [%s] for [User: %s, Amount: %.2f] for print to monthly statements.\n",
                    topic, transaction.getUser(), transaction.getAmount()));
        } else if (topic.equals(TRANSACTIONS_TOPICS.get(1))) {
            System.out.println(String.format("Recording [%s] for [User: %s, Amount: %.2f, Location: %s] for verification tracking.\n",
                    topic, transaction.getUser(), transaction.getAmount(), transaction.getTransactionLocation()));
        } else if (topic.equals(TRANSACTIONS_TOPICS.get(2))) {
            System.out.println(String.format("Recording [%s] for [User: %s, Amount: %.2f, Location: %s] for spending records.\n",
                    topic, transaction.getUser(), transaction.getAmount(), transaction.getTransactionLocation()));
        }

    }

}
