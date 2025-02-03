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
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"; // External listener address

    public static void main(String[] args) {
        // Get configuration with defaults
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS);
        String topic = PropertiesConfig.getProperty("kafka.topics.lab-orders", "lab-orders");

        LOG.info("Connecting to Kafka at: {}", bootstrapServers);
        LOG.info("Publishing to topic: {}", topic);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS); // Always use external address for testing
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Sample HL7 message
        String hl7Message = "MSH|^~\\&|CarePro|Chongwe District Hospital^50030009^URI|DISA*LAB|ZCR|20250127115737||OML^O21^OML_O21|7fba0209-2db9-4821-b1b9-6da639c759e1|P^T|2.5|1|||AL|ZMB\r" +
                "PID|1|5003-0009C-00039-2|5003-0009C-00039-2^^^zm.gov.moh.sc&api_nupn&api_nupn^MR||Test^DISA||19601101|M|||^^Chongwe^^^^^^^^^^20250127115737||766279999||||||||||||||||||||20250127115737\r" +
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
                    LOG.error("Error sending message: {}", exception.getMessage());
                } else {
                    LOG.info("Message sent to partition {} with offset {}",
                            metadata.partition(), metadata.offset());
                }
            });

            producer.flush();
            LOG.info("Test message sent successfully");
        } catch (Exception e) {
            LOG.error("Error in producer: {}", e.getMessage(), e);
            e.printStackTrace();
        }
    }
}