package parcomolo;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    public static void main(String[] args) {

    	String bootstrapServer = "localhost:9092";
    	String groupId = args.length > 0 ? args[0] : "gfg-consumer-group";
    	String topics = "stocks";
    	int sleepPeriod = 250;
    	
        // Create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Kafka Consumer
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(properties);

        // Subscribe Consumer to Our Topics
        consumer.subscribe(List.of(topics));

        // Poll the data
        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {                
                handleStockReservedEvent(record);
            }
            
            try {
            	Thread.sleep(sleepPeriod);
            } catch (Exception ex) {
            	log.error("caught exception", ex);
            }
        }
    }
    
    private static void handleStockReservedEvent(ConsumerRecord<String, String> record) {

        log.info("Key: " + record.key() +
                " Value: " + record.value() +
                " Partition: " + record.partition() +
                " Offset: " + record.offset()
        );

        String orderNumber = record.key();
        
        // process payment...
        
        emitPaymentSuccessfulEvent(orderNumber);
    }

    
    private static void emitPaymentSuccessfulEvent(const String orderNumber) {
    	
    	String bootstrapServers = "localhost:9092";
    	String topicName = "payments"; 
    	
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "20");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(topicName, orderNumber, "successful");
        	
        // Send data
        producer.send(record);
        
        // Tell producer to send all data and block until complete - synchronous
        producer.flush();

        // Close the producer
        producer.close();
    }
    
    private static void emitPaymentFailedEvent(const String orderNumber) {
    	
    	String bootstrapServers = "localhost:9092";
    	String topicName = "payments"; 
    	
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "20");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(topicName, orderNumber, "failed");
        	
        // Send data
        producer.send(record);
        
        // Tell producer to send all data and block until complete - synchronous
        producer.flush();

        // Close the producer
        producer.close();
    }

}
