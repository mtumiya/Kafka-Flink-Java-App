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
    private static final String EXTERNAL_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INTERNAL_BOOTSTRAP_SERVERS = "kafka:29092";

    // Topic groups for different job types
    private static final Map<String, List<String>> JOB_TOPICS = Map.of(
            "lab-orders", Arrays.asList(
                    "lab-orders",
                    "lab-orders-json-extract"
            ),
            "lab-orders-ack", Arrays.asList(
                    "lab-orders-ack",
                    "lab-orders-ack-json-extract"
            ),
            "lab-results", Arrays.asList(
                    "lab-results",
                    "lab-results-json-extract"
            )
    );

    public static boolean setupTopics(String bootstrapServers, String jobType) {
        LOG.info("Attempting to connect to Kafka at {} for job type {}", bootstrapServers, jobType);

        // Get required topics for this job type
        List<String> requiredTopics = JOB_TOPICS.getOrDefault(jobType, Collections.emptyList());
        if (requiredTopics.isEmpty()) {
            LOG.error("No topics defined for job type: {}", jobType);
            return false;
        }

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");

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
                                    TopicConfig.RETENTION_MS_CONFIG, "-1",
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
                LOG.info("All required topics already exist for job type: {}", jobType);
            }

            // Verify final topic list
            Set<String> finalTopics = admin.listTopics().names().get();
            LOG.info("Final topic list: {}", finalTopics);

            // Verify all required topics exist
            return finalTopics.containsAll(requiredTopics);

        } catch (Exception e) {
            LOG.warn("Failed to connect to Kafka at {}: {}", bootstrapServers, e.getMessage());
            return false;
        }
    }

    // Helper method to setup topics with multiple connection attempts
    public static boolean setupTopics(String jobType) {
        // Try with external bootstrap servers first
        if (setupTopics(EXTERNAL_BOOTSTRAP_SERVERS, jobType)) {
            return true;
        }
        // If external fails, try internal
        return setupTopics(INTERNAL_BOOTSTRAP_SERVERS, jobType);
    }
}