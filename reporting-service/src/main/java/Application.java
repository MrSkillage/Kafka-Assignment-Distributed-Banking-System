import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

public class Application {
    // Final String List of the TOPICS and Servers available
    // valid-transaction = 0, suspicious-transaction=1, high-value-transaction=2
    private static final List<String> TOPICS = Collections.unmodifiableList(
            Arrays.asList("valid-transactions","suspicious-transactions","high-value-transactions"));
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    /**
     * Main method call for Application class. Creates new instance of Application. Creates a consumerGroup
     * prints to console the consumerGroup and then creates a new accountConsumer with the BOOTSTRAP_SERVERS and
     * consumerGroup. Calls the consumeMessage function passing the TOPICS and accountConsumer as parameters.
     * @param args
     */
    public static void main(String[] args) {
        // Create a instance of class Application
        Application kafkaReportingNotificationConsumerApp = new Application();
        // String stores the service I.D. of this consumer group
        String consumerGroup = "reporting-service";
        // Print out message of which Consumer Group we belong to
        System.out.println("Consumer is part of consumer group " + consumerGroup + "\n");

        // Call createKafkaConsumer method and pass the Servers and Consumer Group
        Consumer<String, Transaction> reportingConsumer = kafkaReportingNotificationConsumerApp.createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        // Call consumerMessages method and pass the TOPICS and Consumer we created above
        kafkaReportingNotificationConsumerApp.consumeMessages(TOPICS, reportingConsumer);
    }

    /**
     * Takes in two parameters topics and kafkaConsumer and subscribes the topics to the kafkaConsumer.
     * Continues to listen indefinitely and polls the kafkaConsumer into a ConsumerRecords<String, Transaction>.
     * While consumerRecords is not empty loop through each record in consumerRecords and call the function
     * recordTransactionForReporting passing the record topic and record value (Transaction). Commit asynchronous
     * @param topics
     * @param kafkaConsumer
     */
    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        // Subscribe the consumer to the topics list passed
        kafkaConsumer.subscribe(topics);

        // Create and indefinite loop while continuously checking for new messages
        while (true) {
            // ConsumerRecords stores a list of ConsumerRecords and polls every 1 second
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            // If consumerRecords is not empty i.e. has a record then do
            if (!consumerRecords.isEmpty()) {
                // For each ConsumerRecord in the list ConsumerRecords consume message
                for (ConsumerRecord<String, Transaction> record : consumerRecords) {
                    // Call function recordTransactionForReporting passing the record topic and value (Transaction)
                    recordTransactionForReporting(record.topic(), record.value());
                }
            } else {
                // DO NOTHING FOR NOW
            }
            // Tell kafka its done processing messages with a commit as a final confirmation
            kafkaConsumer.commitAsync();
        }
    }

    /**
     * Takes in two parameters bootstrapServers and consumerGroup. Creates a new Properties, prop, and adds the
     * servers (ports), deserializes the <Key, Value> pair, adds the consumerGroup I.D. and sets auto commits to false.
     * Returns a new KafkaConsumer of type <String, Transaction> with the new Properties, prop.
     * @param bootstrapServers
     * @param consumerGroup
     * @return
     */
    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        // Make a new Properties object called prop
        Properties prop = new Properties();
        // Set the properties servers (ports)
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Deserialize the Key (String)
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Deserialize the Value (Transaction)
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        // Configure the ConsumerGroup
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        // Disable auto commit configuration
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Returns a new KafkaConsumer made with the properties we set in prop
        return new KafkaConsumer<String, Transaction>(prop);
    }

    /**
     * Takes in two parameters topic and transaction and based on the topic passed prints
     * out a formatted string to the console with all the needed details of the passed Transaction.
     * @param topic
     * @param transaction
     */
    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // If the topic is a valid-transaction then do the following
        if (topic.equals(TOPICS.get(0))) {
            // Print confirmation message of valid-transaction with formatted transaction details adn topics
            System.out.println(String.format("Recording [%s] for [User: %s, Amount: %.2f] for print to monthly statements.\n",
                    topic, transaction.getUser(), transaction.getAmount()));
        } // Else if the topic is a suspicious-transaction then do the following
        else if (topic.equals(TOPICS.get(1))) {
            // Print confirmation message of suspicious-transaction with formatted transaction details adn topics
            System.out.println(String.format("Recording [%s] for [User: %s, Amount: %.2f, Location: %s] for verification tracking.\n",
                    topic, transaction.getUser(), transaction.getAmount(), transaction.getTransactionLocation()));
        } // Else if the topic is a high-value-transaction then do the following
        else if (topic.equals(TOPICS.get(2))) {
            // Print confirmation message of high-value-transaction with formatted transaction details adn topics
            System.out.println(String.format("Recording [%s] for [User: %s, Amount: %.2f, Location: %s] for spending records.\n",
                    topic, transaction.getUser(), transaction.getAmount(), transaction.getTransactionLocation()));
        }
    }
}
