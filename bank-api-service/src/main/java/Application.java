import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Banking API Service
 */
public class Application {
    // Final String List of the TOPICS and Servers available
    //valid-transaction = 0, suspicious-transaction=1, high-value-transaction=2
    private static final List<String> TOPICS = Collections.unmodifiableList(
            Arrays.asList("valid-transactions","suspicious-transactions","high-value-transactions"));
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    /**
     * Main method call for Application class. Creates new instance of Application. Creates IncomingTransactionsReader,
     * CustomerAddressDatabase. Creates a new kafkaProducer with the BOOTSTRAP_SERVERS
     * Uses a try to processTransactions, catch exceptions and print errors to console, and finally flushes and
     * closes the Producer.
     * @param args
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Create a new instance IncomingTransactionsReader AND CustomerAddressDatabase Object
        IncomingTransactionsReader incomingTransactionsReader = new IncomingTransactionsReader();
        CustomerAddressDatabase customerAddressDatabase = new CustomerAddressDatabase();
        // Create a instance of class Application
        Application kafkaApp = new Application();
        // Call createKafkaProducer method and pass the Servers
        Producer<String, Transaction> kafkaProducer = kafkaApp.createKafkaProducer(BOOTSTRAP_SERVERS);

        // Try to processTransactions passing 3 parameters
        try {
            processTransactions(incomingTransactionsReader, customerAddressDatabase, kafkaProducer);
        } // Catch Execution OR Interrupted Exceptions
        catch (ExecutionException | InterruptedException e) {
            // Print stack trace and error message to console.
            e.printStackTrace();
            System.out.println("ERROR-EXCEPTION - [Failed to Process Transaction, could be a missed Exception!]");
        } // Finally flush and close the Producer
        finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    /**
     * Takes in three parameters incomingTransactionsReader, customerAddressDatabase, and kafkaProducer
     * and throws two Exceptions. Retrieve the next transaction from the incomingTransactionsReader. Get the transaction
     * amount and check if it exceeds the 1000.00 limit. Get the transaction residence can compare it to the users address
     * from customerAddressDatabase. Send a message to the appropriate topic based on the criteria
     * @param incomingTransactionsReader
     * @param customerAddressDatabase
     * @param kafkaProducer
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                           CustomerAddressDatabase customerAddressDatabase,
            Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {
        // While incomingTransactionsReader has another value to read do
        while (incomingTransactionsReader.hasNext()) {
            // Create a new Transaction info, String user, and double value to store Transaction information.
            Transaction info = incomingTransactionsReader.next();
            String user = info.getUser();
            double value = info.getAmount();
            // Creates a currentTopic and printTopic and initializes them.
            String currentTopic = "";
            String printTopic = "";
            // Creates a new ProducerRecord with <String, Transaction>
            ProducerRecord<String, Transaction> record;

            // If the Transaction amount if greater then 1000.00 do
            if (info.getAmount() > 1000.00) {
                // Sets current topic to high-value-transactions
                currentTopic = TOPICS.get(2);
                // If the printTopic is empty add the currentTopic to printTopic
                if (printTopic.isEmpty())
                    printTopic += currentTopic;
                // Else if not empty add a comma break before adding currentTopic to printTopic
                else if (!printTopic.isEmpty())
                    printTopic += ", " + currentTopic;

                // Set record to a new Producer record with the currentTopic, user, and info
                record = new ProducerRecord<>(currentTopic, user, info);
                // Produce the message and send it out to be consumed elsewhere
                kafkaProducer.send(record).get();
            }

            // Checks for valid location in relation to userAddress
            if (info.getTransactionLocation().equals(customerAddressDatabase.getUserResidence(user))) {
                // Sets current topic to valid-transactions
                currentTopic = TOPICS.get(0);
                // If the printTopic is empty add the currentTopic to printTopic
                if (printTopic.isEmpty())
                    printTopic += currentTopic;
                // Else if not empty add a comma break before adding currentTopic to printTopic
                else if (!printTopic.isEmpty())
                    printTopic += ", " + currentTopic;

                // Set record to a new Producer record with the currentTopic, user, and info
                record = new ProducerRecord<>(currentTopic, user, info);
                // Produce the message and send it out to be consumed elsewhere
                kafkaProducer.send(record).get();
            } // Else if the user address does not match the transaction address mark as suspicious-transactions
            else if (!info.getTransactionLocation().equals(customerAddressDatabase.getUserResidence(user))) {
                // Sets current topic to suspicious-transactions
                currentTopic = TOPICS.get(1);
                // If the printTopic is empty add the currentTopic to printTopic
                if (printTopic.isEmpty())
                    printTopic += currentTopic;
                // Else if not empty add a comma break before adding currentTopic to printTopic
                else if (!printTopic.isEmpty())
                    printTopic += ", " + currentTopic;

                // Set record to a new Producer record with the currentTopic, user, and info
                record = new ProducerRecord<>(currentTopic, user, info);
                // Produce the message and send it out to be consumed elsewhere
                kafkaProducer.send(record).get();
            }
            // Print out a formatted message to the console informing the banking-api what type of transaction each is
            System.out.println(String.format("[%s] - [User: %s, Amount: %.2f, Loc: %s, Home: %s]",
                    printTopic, user, value, info.getTransactionLocation(), customerAddressDatabase.getUserResidence(user)));
        }
    }

    /**
     * Takes in one parameter bootstrapServers. Creates a new Properties, prop, and adds the
     * servers (ports), serializes the <Key, Value> pair, adds the Client I.D.
     * Returns a new KafkaProducer of type <String, Transaction> with the new Properties, prop.
     * @param bootstrapServers
     * @return
     */
    public Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        // Make a new Properties object called prop
        Properties prop = new Properties();
        // Set the properties servers (ports)
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Configure the Producer client I.D.
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, "banking-api");
        // Serialize the Key (String)
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Serialize the Value (Transaction)
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName());
        // Returns a new KafkaConsumer made with the properties we set in prop
        return new KafkaProducer<String, Transaction>(prop);
    }
}
