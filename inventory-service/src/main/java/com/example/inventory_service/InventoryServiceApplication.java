package com.example.inventory_service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@SpringBootApplication(scanBasePackages = {
	"com.example.inventory_service"
})
@EnableJpaRepositories("com.example.inventory_service")
@EntityScan("com.example.inventory_service.*")
public class InventoryServiceApplication {

	private static final Logger log = LoggerFactory.getLogger(InventoryServiceApplication.class);
//
//	@Autowired
//	private static BananaRepository bananaRepository;

	private static final Map<String, String> bananaInventory = Map.of("banana", "banana",
		"banana2", "banana",
		"banana3", "banana",
		"banana4", "banana",
		"banana5", "banana",
		"banana6", "banana",
		"banana7", "banana",
		"banana8", "banana",
		"banana9", "banana",
		"banana0", "banana");
	private static Integer inventorySize = bananaInventory.size();

	public static void main(String[] args) {
		SpringApplication.run(InventoryServiceApplication.class, args);
		System.out.println("Inventory Service Application Started");


		Properties consumerProps = new Properties();
		consumerProps.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		consumerProps.setProperty(GROUP_ID_CONFIG, "my-first-consumer");
		consumerProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "true");
		consumerProps.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		consumerProps.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		Properties producerProps = new Properties();
		producerProps.put("bootstrap.servers", "localhost:9092");
		producerProps.put("key.serializer", StringSerializer.class.getName());
		producerProps.put("value.serializer", StringSerializer.class.getName());

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
			 Producer<String, String> producer = new KafkaProducer<>(producerProps);) {
			consumer.subscribe(Collections.singletonList("orders"));

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					log.info(
						"partition={}, offset = {}, key = {}, value = {}",
						record.partition(),
						record.offset(),
						record.key(),
						record.value()
					);
					String orderStatus = record.value();
					log.info("Order status is PENDING");

					if (orderStatus.equals("PENDING") && inventorySize > 0) {
						log.info("There is a banana!!!");
						inventorySize--;

						producer.send(new ProducerRecord<>("stocks", record.key(), orderStatus));
						log.info("Stock status is PENDING");
					}
				}
			}
		}
	}

}
