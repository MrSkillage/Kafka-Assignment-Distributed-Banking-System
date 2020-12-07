import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Banking API Service
 */
public class Application {
    private static final String VALID_TOPIC = "valid-transactions";
    private static final String SUSPICIOUS_TOPIC = "suspicious-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        IncomingTransactionsReader incomingTransactionsReader = new IncomingTransactionsReader();
        CustomerAddressDatabase customerAddressDatabase = new CustomerAddressDatabase();

        Application kafkaApp = new Application();
        Producer<String, Transaction> kafkaProducer = kafkaApp.createKafkaProducer(BOOTSTRAP_SERVERS);

        try {
            processTransactions(incomingTransactionsReader, customerAddressDatabase, kafkaProducer);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }


    }

    public static void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                           CustomerAddressDatabase customerAddressDatabase,
            Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {


        // Retrieve the next transaction from the IncomingTransactionsReader
        // For the transaction user, get the user residence from the UserResidenceDatabase
        // Compare user residence to transaction location.
        // Send a message to the appropriate topic, depending on whether the user residence and transaction
        // location match or not.
        // Print record metadata information

        while (incomingTransactionsReader.hasNext()) {

            Transaction info = incomingTransactionsReader.next();
            String user = info.getUser();
            double value = info.getAmount();

            if (info.getTransactionLocation().equals(customerAddressDatabase.getUserResidence(user))) {

                ProducerRecord<String, Transaction> record = new ProducerRecord<>(VALID_TOPIC, user, info);

                RecordMetadata recordMetadata = kafkaProducer.send(record).get();

                System.out.println(String.format("[User: %s, Amount: %f, Loc: %s, Home: %s, MATCHED!",
                        user, value, info.getTransactionLocation(), customerAddressDatabase.getUserResidence(user)));


            } else {

                ProducerRecord<String, Transaction> record = new ProducerRecord<>(SUSPICIOUS_TOPIC, user, info);

                RecordMetadata recordMetadata = kafkaProducer.send(record).get();

                System.out.println(String.format("[User: %s, Amount: %f, Loc: %s, Home: %s, SUSPICIOUS!",
                        user, value, info.getTransactionLocation(), customerAddressDatabase.getUserResidence(user)));
            }

        }


    }

    public Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        // Make a new Properties object called prop
        Properties prop = new Properties();

        // Set the various Properties used to setup a Producer Configuration
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "banking-api");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName());

        // Returns a new KafkaConsumer made with the properties we set in prop
        return new KafkaProducer<String, Transaction>(prop);
    }

}
