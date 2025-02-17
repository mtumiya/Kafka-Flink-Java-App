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
        String topic = PropertiesConfig.getProperty("kafka.topics.lab-results", "lab-results");
        // Use localhost:9092 for development, get from config for production
        String bootstrapServers = "prod".equalsIgnoreCase(System.getenv("SPRING_PROFILES_ACTIVE"))
                ? PropertiesConfig.getProperty("kafka.bootstrap.servers")
                : "localhost:9092";

        LOG.info("Connecting to Kafka at: {}", bootstrapServers);
        LOG.info("Publishing to topic: {}", topic);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Add security properties if in production
        if ("prod".equalsIgnoreCase(System.getenv("SPRING_PROFILES_ACTIVE"))) {
            LOG.info("Adding production security configuration");
            props.putAll(PropertiesConfig.getKafkaSecurityProperties());
        }

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
                    LOG.error("Error sending result message: {}", exception.getMessage());
                } else {
                    LOG.info("Result message sent to partition {} with offset {}",
                            metadata.partition(), metadata.offset());
                }
            });

            producer.flush();
            LOG.info("Test result message sent successfully");
        } catch (Exception e) {
            LOG.error("Error in producer: {}", e.getMessage());
            e.printStackTrace();
        }
    }
}