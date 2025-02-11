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

public class LabOrderAckProcessingJob {
    private static final Logger LOG = LoggerFactory.getLogger(LabOrderAckProcessingJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Lab Order Acknowledgment Processing Job");

        // Setup Kafka topics
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        LOG.info("Setting up Kafka topics using bootstrap servers: {}", bootstrapServers);
        if (!KafkaTopicSetup.setupTopics("lab-orders-ack")) {
            LOG.error("Failed to set up Kafka topics. Please check your Kafka configuration.");
            System.exit(1);
        }
        LOG.info("Kafka topics setup completed successfully");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnvironment(env);

        // Create and configure pipeline
        DataStream<LabOrderAck> labOrderAckStream = createLabOrderAcksStream(env);
        configureLabOrderAcksPipeline(labOrderAckStream);

        LOG.info("Submitting Flink job");
        env.execute("Lab Order Acknowledgment Processing Job");
    }

    private static void configureEnvironment(StreamExecutionEnvironment env) {
        LOG.info("Configuring Flink environment");

        // Similar environment configuration as LabOrderProcessingJob
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 10000));

        LOG.info("Environment configured successfully");
    }

    private static DataStream<LabOrderAck> createLabOrderAcksStream(StreamExecutionEnvironment env) {
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        String consumerGroup = PropertiesConfig.getProperty("kafka.consumer.group");
        String topic = PropertiesConfig.getProperty("kafka.topics.lab-orders-ack");

        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("isolation.level", "read_committed");
        sourceProperties.setProperty("auto.offset.reset", "earliest");

        LOG.info("Creating lab order acknowledgments stream from topic: {}", topic);

        KafkaSource<LabOrderAck> source = KafkaSource.<LabOrderAck>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new HL7DeserializationSchema.HL7AckDeserializationSchema())
                .setProperties(sourceProperties)
                .build();

        return env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka HL7 ACK Source")
                .uid("kafka-hl7-ack-source")
                .name("Kafka HL7 ACK Source")
                .filter(ack -> ack != null && ack.getAckMessageId() != null)
                .uid("lab-orders-ack-filter")
                .name("Lab Orders ACK Filter");
    }

    private static void configureLabOrderAcksPipeline(DataStream<LabOrderAck> labOrderAckStream) {
        LOG.info("Configuring lab order acknowledgments pipeline");
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        String jsonTopic = PropertiesConfig.getProperty("kafka.topics.lab-orders-ack-json");

        // Configure Kafka producer properties
        Properties producerConfig = new Properties();
        producerConfig.setProperty("transaction.timeout.ms", "3600000");
        producerConfig.setProperty("max.block.ms", "60000");
        producerConfig.setProperty("request.timeout.ms", "60000");
        producerConfig.setProperty("retries", "3");
        producerConfig.setProperty("retry.backoff.ms", "1000");
        producerConfig.setProperty("enable.idempotence", "true");

        KafkaSink<LabOrderAck> jsonSink = KafkaSink.<LabOrderAck>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<LabOrderAck>builder()
                        .setTopic(jsonTopic)
                        .setValueSerializationSchema(new JsonSerializationSchemas.LabOrderAckSerializationSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerConfig)
                .build();

        labOrderAckStream
                .sinkTo(jsonSink)
                .uid("kafka-ack-json-sink")
                .name("Kafka Lab Orders ACK JSON Sink");

        String dbUrl = PropertiesConfig.getProperty("spring.datasource.url");
        String dbUsername = PropertiesConfig.getProperty("spring.datasource.username");
        String dbPassword = PropertiesConfig.getProperty("spring.datasource.password");

        labOrderAckStream
                .addSink(new PostgresSinkSchemas.LabOrderAckSink(dbUrl, dbUsername, dbPassword))
                .uid("postgres-lab-orders-ack-sink")
                .name("PostgreSQL Lab Orders ACK Sink");
    }
}