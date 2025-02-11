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

public class LabOrderProcessingJob {
    private static final Logger LOG = LoggerFactory.getLogger(LabOrderProcessingJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Lab Order Processing Job");

        // Setup Kafka topics
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        LOG.info("Setting up Kafka topics using bootstrap servers: {}", bootstrapServers);
        if (!KafkaTopicSetup.setupTopics("lab-orders")) {
            LOG.error("Failed to set up Kafka topics. Please check your Kafka configuration.");
            System.exit(1);
        }
        LOG.info("Kafka topics setup completed successfully");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnvironment(env);

        // Create and configure pipeline
        DataStream<LabOrder> labOrderStream = createLabOrdersStream(env);
        configureLabOrdersPipeline(labOrderStream);

        LOG.info("Submitting Flink job");
        env.execute("Lab Order Processing Job");
    }

    private static void configureEnvironment(StreamExecutionEnvironment env) {
        LOG.info("Configuring Flink environment");

        // Reduced checkpoint interval and increased restart attempts
        env.enableCheckpointing(5000); // 5 seconds
        env.getCheckpointConfig().setCheckpointTimeout(60000); // 1 minute
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000); // 2 seconds
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 10000)); // 5 attempts, 10 second delay

        LOG.info("Environment configured successfully");
    }

    private static DataStream<LabOrder> createLabOrdersStream(StreamExecutionEnvironment env) {
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        String consumerGroup = PropertiesConfig.getProperty("kafka.consumer.group");
        String topic = PropertiesConfig.getProperty("kafka.topics.lab-orders");

        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("isolation.level", "read_committed");
        sourceProperties.setProperty("auto.offset.reset", "earliest");

        LOG.info("Creating lab orders stream from topic: {}", topic);

        KafkaSource<LabOrder> source = KafkaSource.<LabOrder>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new HL7DeserializationSchema.HL7MessageDeserializationSchema())
                .setProperties(sourceProperties)
                .build();

        return env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka HL7 Source")
                .uid("kafka-hl7-source")
                .name("Kafka HL7 Source")
                .filter(order -> order != null && order.getLabOrderId() != null)
                .uid("lab-orders-filter")
                .name("Lab Orders Filter");
    }

    private static void configureLabOrdersPipeline(DataStream<LabOrder> labOrderStream) {
        LOG.info("Configuring lab orders pipeline");
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        String jsonTopic = PropertiesConfig.getProperty("kafka.topics.lab-orders-json");

        // Configure Kafka producer properties
        Properties producerConfig = new Properties();
        producerConfig.setProperty("transaction.timeout.ms", "3600000"); // 1 hour
        producerConfig.setProperty("max.block.ms", "60000");
        producerConfig.setProperty("request.timeout.ms", "60000");
        producerConfig.setProperty("retries", "3");
        producerConfig.setProperty("retry.backoff.ms", "1000");
        producerConfig.setProperty("enable.idempotence", "true");

        KafkaSink<LabOrder> jsonSink = KafkaSink.<LabOrder>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<LabOrder>builder()
                        .setTopic(jsonTopic)
                        .setValueSerializationSchema(new JsonSerializationSchemas.LabOrderSerializationSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerConfig)
                .build();

        labOrderStream
                .sinkTo(jsonSink)
                .uid("kafka-json-sink")
                .name("Kafka Lab Orders JSON Sink");

        String dbUrl = PropertiesConfig.getProperty("spring.datasource.url");
        String dbUsername = PropertiesConfig.getProperty("spring.datasource.username");
        String dbPassword = PropertiesConfig.getProperty("spring.datasource.password");

        labOrderStream
                .addSink(new PostgresSinkSchemas.LabOrderSink(dbUrl, dbUsername, dbPassword))
                .uid("postgres-sink")
                .name("PostgreSQL Lab Orders Sink");
    }
}