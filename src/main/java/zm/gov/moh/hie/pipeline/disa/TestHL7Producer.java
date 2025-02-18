package zm.gov.moh.hie.pipeline.disa;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TestHL7Producer {
    private static final Logger LOG = LoggerFactory.getLogger(TestHL7Producer.class);

    public static void main(String[] args) {
        LOG.info("Starting TestHL7Producer...");
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

        String topic = PropertiesConfig.getProperty("kafka.topics.lab-orders", "lab-orders");
        LOG.info("Target topic: {}", topic);

        // Log all producer properties (excluding sensitive info)
        LOG.info("Producer Properties:");
        props.stringPropertyNames().stream()
                .filter(key -> !key.contains("password") && !key.contains("secret"))
                .forEach(key -> LOG.info("{}={}", key, props.getProperty(key)));

        // Sample HL7 message
        String hl7Message = "MSH|^~\\&|CarePro|Chongwe District Hospital^50030009^URI|DISA*LAB|ZCR|20250127115737||OML^O21^OML_O21|" +
                "7fba0209-2db9-4821-b1b9-6da639c759e1|P^T|2.5|1|||AL|ZMB\r" +
                "PID|1|5003-0009C-00039-2|5003-0009C-00039-2^^^zm.gov.moh.sc&api_nupn&api_nupn^MR||Test^DISA||19601101|M|||" +
                "^^Chongwe^^^^^^^^^^20250127115737||766279999||||||||||||||||||||20250127115737\r" +
                "PV1|1|O|50030009^^^^^^^^Chongwe District Hospital|||||111111/11/1^Administrator^System||||||||||||||||||||||||||||||||||||20250127115737\r" +
                "ORC|NW|51A9DF0291|||||||20250127155100\r" +
                "OBR|1|51A9DF0291||47245-6^Viral Load^LOINC|Regular|20250127115737|20250127115737\r" +
                "OBX|1|ST|VISIT^Visit Type||3M||||||F\r" +
                "OBR|2|51A9DF0291||20447-9^Viral Load^LOINC|Regular|20250127115737|20250127115737\r" +
                "SPM|1|||B^Blood|||||||||||||20250127155100|20250127155100";

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, hl7Message);

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