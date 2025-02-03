package zm.gov.moh.hie.pipeline.disa;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaTopicSetup {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicSetup.class);
    private static final String EXTERNAL_BOOTSTRAP_SERVERS = "localhost:9092"; // For local testing
    private static final String INTERNAL_BOOTSTRAP_SERVERS = "kafka:29092";    // For Docker network

    public static void main(String[] args) {
        // Try with external bootstrap servers first
        if (!setupTopics(EXTERNAL_BOOTSTRAP_SERVERS)) {
            // If external fails, try internal
            if (!setupTopics(INTERNAL_BOOTSTRAP_SERVERS)) {
                System.exit(1);
            }
        }
    }

    public static boolean setupTopics(String bootstrapServers) {
        LOG.info("Attempting to connect to Kafka at {}", bootstrapServers);

        // Get all topic names from properties
        List<String> requiredTopics = Arrays.asList(
                "lab-orders",
                "lab-orders-ack",
                "lab-results",
                "lab-orders-json-extract",
                "lab-orders-ack-json-extract",
                "lab-results-json-extract"
        );

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000"); // 5 second timeout

        try (Admin admin = Admin.create(props)) {
            // Check existing topics
            ListTopicsResult listTopics = admin.listTopics();
            Set<String> existingTopics = listTopics.names().get();
            LOG.info("Existing topics: {}", existingTopics);

            // Create new topics configurations
            List<NewTopic> newTopics = new ArrayList<>();
            for (String topicName : requiredTopics) {
                if (!existingTopics.contains(topicName)) {
                    NewTopic newTopic = new NewTopic(topicName, 1, (short) 1)
                            .configs(Map.of(
                                    TopicConfig.RETENTION_MS_CONFIG, "-1",  // Infinite retention
                                    TopicConfig.CLEANUP_POLICY_CONFIG, "delete"
                            ));
                    newTopics.add(newTopic);
                    LOG.info("Preparing to create topic: {}", topicName);
                }
            }

            if (!newTopics.isEmpty()) {
                // Create the topics
                CreateTopicsResult result = admin.createTopics(newTopics);

                // Wait for topic creation to complete
                for (Map.Entry<String, KafkaFuture<Void>> entry : result.values().entrySet()) {
                    try {
                        entry.getValue().get();
                        LOG.info("Successfully created topic: {}", entry.getKey());
                    } catch (ExecutionException e) {
                        LOG.error("Failed to create topic {}: {}", entry.getKey(), e.getMessage());
                    }
                }
            } else {
                LOG.info("All required topics already exist");
            }

            // Verify final topic list
            Set<String> finalTopics = admin.listTopics().names().get();
            LOG.info("Final topic list: {}", finalTopics);

            return true;

        } catch (Exception e) {
            LOG.warn("Failed to connect to Kafka at {}: {}", bootstrapServers, e.getMessage());
            return false;
        }
    }
}