package zm.gov.moh.hie.pipeline.disa;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TestHL7ResultProducer {
    private static final Logger LOG = LoggerFactory.getLogger(TestHL7ResultProducer.class);

    public static void main(String[] args) {
        LOG.info("Starting TestHL7ResultProducer...");
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

        String topic = PropertiesConfig.getProperty("kafka.topics.lab-results", "lab-results");
        LOG.info("Target topic: {}", topic);

        // Log all producer properties (excluding sensitive info)
        LOG.info("Producer Properties:");
        props.stringPropertyNames().stream()
                .filter(key -> !key.contains("password") && !key.contains("secret"))
                .forEach(key -> LOG.info("{}={}", key, props.getProperty(key)));

        // Sample HL7 result message
        String hl7ResultMessage = "MSH|^~\\&|DISA*LAB|Minga Mission Hospital^ZMG^URI|SmartCare|1969|20250127111249||ORU^R01^ORU_R01|3AD623C1-B49D-4A2F-B5C2-170E875A79A5|P^T|2.5||||AL|ZMB\r" +
                "SFT|Laboratory System Technologies Pty (Ltd)|05.05.03.1323|DisaHL7Rslt||Disa*Lab|20230220|\r" +
                "PID|1||3000-30026-00197-7^^^^MR||BANDA^BEATRICE^^|||F||||||||||||||||||||||N||\r" +
                "NTE|1|L|SPECDT:2024122300:00 F RCVDT:0000000000:00 F Priority:R|\r" +
                "NTE|2|L|REGBY:    F 2025012212:17:51 F MODBY: F 0000000000:00|\r" +
                "PV1|1|U|^^^30030026^^^^^||||||||||||||||||||||||||||||||||||||||||\r" +
                "ORC|SC|51A9DF0291|ZMG0231281||CM||||||||||||||||||||||||||\r" +
                "OBR|1|51A9DF0291|ZMG0231281|20447-9^HIVVL^LN^HIVVL^HIVVL^L|||202501241907|||||^||202501221217|P^^Plasma|^||||||202501271103|||F|||||||202501271103||202501271103\r" +
                "OBX|1|ST| ||HIVVL HIV VIRAL LOAD||||||F\r" +
                "OBX|2|NM|20447-9^HIV Viral Load^LN^HIVVL^HIV Viral Load^L||388|copies/mL|0-0||||F|||202501241907|^ZMG|||C4800";

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, hl7ResultMessage);

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