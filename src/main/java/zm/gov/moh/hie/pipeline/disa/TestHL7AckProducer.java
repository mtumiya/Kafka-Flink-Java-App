package zm.gov.moh.hie.pipeline.disa;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TestHL7AckProducer {
    private static final Logger LOG = LoggerFactory.getLogger(TestHL7AckProducer.class);
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"; // External listener address

    public static void main(String[] args) {
        // Get configuration with defaults
        String topic = PropertiesConfig.getProperty("kafka.topics.lab-orders-ack", "lab-orders-ack");

        LOG.info("Connecting to Kafka at: {}", DEFAULT_BOOTSTRAP_SERVERS);
        LOG.info("Publishing to topic: {}", topic);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Sample ACK message
        String hl7AckMessage = "MSH|^~\\&|DISA*LAB|ZCR|SmartCare|50030009|20250127115737||ACK^O21^ACK|7fba0209-2db9-4821-b1b9-6da639c759e1|P^T|2.5||||NE|ZMB\r" +
                "MSA|AA|4fb6df6f-7f49-4bf5-ab43-6364a0eea574|Order successfully processed|";

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, hl7AckMessage);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Error sending ACK message: {}", exception.getMessage());
                } else {
                    LOG.info("ACK message sent to partition {} with offset {}",
                            metadata.partition(), metadata.offset());
                }
            });

            producer.flush();
            LOG.info("ACK test message sent successfully");
        } catch (Exception e) {
            LOG.error("Error in producer: {}", e.getMessage());
            e.printStackTrace();
        }
    }
}