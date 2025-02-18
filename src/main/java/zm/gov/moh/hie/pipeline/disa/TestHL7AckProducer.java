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

    public static void main(String[] args) {
        LOG.info("Starting TestHL7AckProducer...");
        LOG.info("Java Version: {}", System.getProperty("java.version"));
        LOG.info("Current Working Directory: {}", System.getProperty("user.dir"));
        LOG.info("SPRING_PROFILES_ACTIVE: {}", System.getenv("SPRING_PROFILES_ACTIVE"));

        boolean isProd = "prod".equalsIgnoreCase(System.getenv("SPRING_PROFILES_ACTIVE"));

        // Get the proper bootstrap servers based on environment
        String bootstrapServers;
        Properties props = new Properties();

        if (isProd) {
            LOG.info("Initializing PRODUCTION configuration");
            bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
            Properties securityProps = PropertiesConfig.getKafkaSecurityProperties();

            LOG.info("Production Configuration:");
            LOG.info("Bootstrap Servers: {}", bootstrapServers);
            LOG.info("Security Protocol: {}", securityProps.getProperty("security.protocol"));
            LOG.info("SASL Mechanism: {}", securityProps.getProperty("sasl.mechanism"));

            props.putAll(securityProps);
        } else {
            bootstrapServers = "localhost:9092";
            LOG.info("Running in DEVELOPMENT mode with bootstrap servers: {}", bootstrapServers);
        }

        // Set basic producer properties
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");

        String topic = PropertiesConfig.getProperty("kafka.topics.lab-orders-ack", "lab-orders-ack");
        LOG.info("Target topic: {}", topic);

        // Log all producer properties (excluding sensitive info)
        LOG.info("Producer Properties:");
        props.stringPropertyNames().stream()
                .filter(key -> !key.contains("password") && !key.contains("secret"))
                .forEach(key -> LOG.info("{}={}", key, props.getProperty(key)));

        // Sample ACK message
        String hl7AckMessage = "MSH|^~\\&|DISA*LAB|ZCR|SmartCare|50030009|20250127115737||ACK^O21^ACK|7fba0209-2db9-4821-b1b9-6da639c759e1|P^T|2.5||||NE|ZMB\r" +
                "MSA|AA|4fb6df6f-7f49-4bf5-ab43-6364a0eea574|Order successfully processed|";

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, hl7AckMessage);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Error sending message:", exception);
                    LOG.error("Error details: {}", exception.getMessage());
                    if (exception.getCause() != null) {
                        LOG.error("Cause: {}", exception.getCause().getMessage());
                    }
                } else {
                    LOG.info("Message successfully sent:");
                    LOG.info("Topic: {}", metadata.topic());
                    LOG.info("Partition: {}", metadata.partition());
                    LOG.info("Offset: {}", metadata.offset());
                    LOG.info("Timestamp: {}", metadata.timestamp());
                }
            });

            producer.flush();
            LOG.info("Producer flush completed");
        } catch (Exception e) {
            LOG.error("Critical error in producer:", e);
            System.exit(1);
        }
    }
}