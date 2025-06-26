package order.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class OrderService {

    private final Map<Integer, Order> orders = new HashMap<>();
    private int nextId = 1;

    public static void main(String[] args) {
        SpringApplication.run(OrderService.class, args);
    }
    
    @GetMapping("/list-orders")
    public Collection<Order> listOrders() {
        return orders.values();
    }

    @PostMapping("/orders")
    public String placeOrderCommand() {
        int orderId = nextId++;
        Order order = new Order(orderId, "Banana", Order.Status.PENDING);
        orders.put(orderId, order);
        
        sendOrderKafka(order);
        
        System.out.println("[Kafka] OrderPlacedEvent - ID: " + orderId);
        return "Order created with ID: " + orderId;
    }

    @PostMapping("/cancelOrder")
    public String cancelOrderCommand(@RequestParam int id) {
        Order order = orders.get(id);
        if (order != null && order.status == Order.Status.PENDING) {
            order.status = Order.Status.CANCELLED;

            deleteOrderKafka(id);

            System.out.println("[Kafka] OrderCancelled - ID: " + id);
            return "Order " + id + " cancelled and removed.";
        } else {
            return "Order not found or already cancelled.";
        }
    }
    
    @PutMapping("/orders/{id}/cancel")
    public String cancelOrder(@PathVariable int id) {
        Order order = orders.get(id);
        if (order != null && order.status == Order.Status.PENDING) {
            order.status = Order.Status.CANCELLED;

            deleteOrderKafka(id);

            System.out.println("[Kafka] OrderCancelled - ID: " + id);
            return "Order " + id + " cancelled and removed.";
        } else {
            return "Order not found or already cancelled from persistance.";
        }
    }

    void sendOrderKafka(Order order) {
    	Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "0");
        props.put("batch.size", "2000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        System.out.println("New Order " + order.id + " of type " + order.product + " ;state" + order.status.toString() );
        
        producer.send(new ProducerRecord<>("orders", String.valueOf(order.id), order.status.toString()));
        
        producer.close();
    }
    
    void deleteOrderKafka(int id) {
    	Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "0");
        props.put("batch.size", "2000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        producer.send(new ProducerRecord<>("orders", String.valueOf(id), Order.Status.CANCELLED.toString()));
        
        producer.close();
    }

}
