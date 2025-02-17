package zm.gov.moh.hie.pipeline.disa;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class LabResultProcessingJob {
    private static final Logger LOG = LoggerFactory.getLogger(LabResultProcessingJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Lab Result Processing Job");

        // Setup Kafka topics
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        LOG.info("Setting up Kafka topics using bootstrap servers: {}", bootstrapServers);
        if (!KafkaTopicSetup.setupTopics("lab-results")) {
            LOG.error("Failed to set up Kafka topics. Please check your Kafka configuration.");
            System.exit(1);
        }
        LOG.info("Kafka topics setup completed successfully");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnvironment(env);

        // Create and configure pipeline
        DataStream<LabResult> labResultStream = createLabResultsStream(env);
        configureLabResultsPipeline(labResultStream);

        LOG.info("Submitting Flink job");
        env.execute("Lab Result Processing Job");
    }

    private static void configureEnvironment(StreamExecutionEnvironment env) {
        LOG.info("Configuring Flink environment");

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 10000));

        LOG.info("Environment configured successfully");
    }

    private static DataStream<LabResult> createLabResultsStream(StreamExecutionEnvironment env) {
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        String consumerGroup = PropertiesConfig.getProperty("kafka.consumer.group");
        String topic = PropertiesConfig.getProperty("kafka.topics.lab-results");

        // Get base Kafka properties and add security properties
        Properties sourceProperties = new Properties();
        sourceProperties.putAll(PropertiesConfig.getKafkaSecurityProperties());
        sourceProperties.setProperty("isolation.level", "read_committed");
        sourceProperties.setProperty("auto.offset.reset", "earliest");

        LOG.info("Creating lab results stream from topic: {}", topic);

        KafkaSource<LabResult> source = KafkaSource.<LabResult>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new HL7DeserializationSchema.HL7ResultDeserializationSchema())
                .setProperties(sourceProperties)
                .build();

        return env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka HL7 Results Source")
                .uid("kafka-hl7-results-source")
                .name("Kafka HL7 Results Source")
                .filter(result -> result != null && result.getLabOrderId() != null)
                .uid("lab-results-filter")
                .name("Lab Results Filter");
    }

    private static void configureLabResultsPipeline(DataStream<LabResult> labResultStream) {
        LOG.info("Configuring lab results pipeline");
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        String jsonTopic = PropertiesConfig.getProperty("kafka.topics.lab-results-json");

        // Configure Kafka producer properties with security
        Properties producerConfig = new Properties();
        producerConfig.putAll(PropertiesConfig.getKafkaSecurityProperties());
        producerConfig.setProperty("transaction.timeout.ms", "3600000");
        producerConfig.setProperty("max.block.ms", "60000");
        producerConfig.setProperty("request.timeout.ms", "60000");
        producerConfig.setProperty("retries", "3");
        producerConfig.setProperty("retry.backoff.ms", "1000");
        producerConfig.setProperty("enable.idempotence", "true");

        KafkaSink<LabResult> jsonSink = KafkaSink.<LabResult>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<LabResult>builder()
                        .setTopic(jsonTopic)
                        .setValueSerializationSchema(new JsonSerializationSchemas.LabResultSerializationSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerConfig)
                .build();

        labResultStream
                .sinkTo(jsonSink)
                .uid("kafka-results-json-sink")
                .name("Kafka Lab Results JSON Sink");

        String dbUrl = PropertiesConfig.getProperty("spring.datasource.url");
        String dbUsername = PropertiesConfig.getProperty("spring.datasource.username");
        String dbPassword = PropertiesConfig.getProperty("spring.datasource.password");

        labResultStream
                .addSink(new PostgresSinkSchemas.LabResultSink(dbUrl, dbUsername, dbPassword))
                .uid("postgres-lab-results-sink")
                .name("PostgreSQL Lab Results Sink");
    }
}