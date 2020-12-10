import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Application {
    // Final Strings of the Topic and Servers available
    private static final String TOPIC = "high-value-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    /**
     * Main method call for Application class. Creates new instance of Application. Creates a consumerGroup
     * prints to console the consumerGroup and then creates a new accountConsumer with the BOOTSTRAP_SERVERS and
     * consumerGroup. Calls the consumeMessage function passing the TOPIC and accountConsumer as parameters.
     * @param args
     */
    public static void main(String[] args) {
        // Create a instance of class Application
        Application kafkaHighValueNotificationConsumerApp = new Application();
        // String stores the service I.D. of this consumer group
        String consumerGroup = "high-value-service";
        // Print out message of which Consumer Group we belong to
        System.out.println("Consumer is part of consumer group " + consumerGroup + "\n");

        // Call createKafkaConsumer method and pass the Servers and Consumer Group
        Consumer<String, Transaction> highValueConsumer = kafkaHighValueNotificationConsumerApp.createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        // Call consumerMessages method and pass the TOPIC and Consumer we created above
        kafkaHighValueNotificationConsumerApp.consumeMessages(TOPIC, highValueConsumer);
    }

    /**
     * Takes in two parameters topics and kafkaConsumer and subscribes the topics to the kafkaConsumer.
     * Continues to listen indefinitely and polls the kafkaConsumer into a ConsumerRecords<String, Transaction>.
     * While consumerRecords is not empty loop through each record in consumerRecords and call the function
     * highValueTransactionForReporting passing the record topic and record value (Transaction). Commit asynchronous
     * @param topic
     * @param kafkaConsumer
     */
    public static void consumeMessages(String topic, Consumer<String, Transaction> kafkaConsumer) {
        // Subscribe the consumer to the topic passed in list format
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        // Create an indefinite loop while continuously checking for new messages
        while (true) {
            // ConsumerRecords stores a list of ConsumerRecords and polls every 1 second
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            // If consumerRecords is not empty i.e. has a record then do
            if (!consumerRecords.isEmpty()) {
                // For each ConsumerRecord in the list ConsumerRecords consume message
                for (ConsumerRecord<String, Transaction> record : consumerRecords) {
                    // Call function highValueTransactionForReporting passing the record topic and value (Transaction)
                    highValueTransactionForReporting(record.topic(), record.value());
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
     * Takes in two parameters topic and transaction and prints out a formatted string to the console
     * with all the needed details of the passed Transaction.
     * @param topic
     * @param transaction
     */
    private static void highValueTransactionForReporting(String topic, Transaction transaction) {
        // Print confirmation message of high-value-transaction with formatted transaction details adn topics
        System.out.println(String.format("Recording [%s] for [User: %s, Amount: %.2f, Location: %s] Bank Threshold: [1000.00]" +
                        ", Threshold Difference: [%.2f]\n",
                topic, transaction.getUser(), transaction.getAmount(), transaction.getTransactionLocation(),
                (transaction.getAmount() - 1000.00)));
    }
}
