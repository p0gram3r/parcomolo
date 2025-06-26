package kafkaworkshop;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ShippingProducer {
    private static final Logger log = LoggerFactory.getLogger(ShippingProducer.class);

    public static void processShipping (String orderId) throws InterruptedException {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ACKS_CONFIG, "all");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        String shipmentId = UUID.randomUUID().toString();
        Shipment newShipment = new Shipment(orderId, shipmentId, Shipment.Status.scheduled);
        log.info("Scheduled shipment with ID: " + shipmentId);
        producer.send(new ProducerRecord<>("scheduledShipments", orderId, shipmentId));
        Thread.sleep(3000);
        newShipment.setStatus(Shipment.Status.shipped);
        log.info("Shipped shipment with ID: " + shipmentId);
        producer.send(new ProducerRecord<>("shippedItems", orderId, newShipment.getShipmentID()));
        producer.close();
    }

}

