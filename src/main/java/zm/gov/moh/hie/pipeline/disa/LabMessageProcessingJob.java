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

public class LabMessageProcessingJob {
    private static final Logger LOG = LoggerFactory.getLogger(LabMessageProcessingJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Lab Message Processing Job");

        // Add Kafka topic setup
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        LOG.info("Setting up Kafka topics using bootstrap servers: {}", bootstrapServers);
        if (!KafkaTopicSetup.setupTopics(bootstrapServers)) {
            LOG.error("Failed to set up Kafka topics. Please check your Kafka configuration.");
            System.exit(1);
        }
        LOG.info("Kafka topics setup completed successfully");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        configureEnvironment(env);

        // Create and configure pipelines
        DataStream<LabOrder> labOrderStream = createLabOrdersStream(env);
        configureLabOrdersPipeline(labOrderStream);

        DataStream<LabOrderAck> labOrderAckStream = createLabOrderAcksStream(env);
        configureLabOrderAcksPipeline(labOrderAckStream);

        DataStream<LabResult> labResultStream = createLabResultsStream(env);
        configureLabResultsPipeline(labResultStream);

        LOG.info("Submitting Flink job");
        env.execute("Lab Message Processing Job");
    }

    private static void configureEnvironment(StreamExecutionEnvironment env) {
        LOG.info("Configuring Flink environment");

        int checkpointInterval = PropertiesConfig.getIntProperty("flink.checkpoint.interval", 10000);
        int checkpointTimeout = PropertiesConfig.getIntProperty("flink.checkpoint.timeout", 60000);
        int minPauseBetweenCheckpoints = PropertiesConfig.getIntProperty("flink.checkpoint.min.pause", 5000);
        int maxConcurrentCheckpoints = PropertiesConfig.getIntProperty("flink.checkpoint.max.concurrent", 1);
        int restartAttempts = PropertiesConfig.getIntProperty("flink.restart.attempts", 3);
        int restartDelay = PropertiesConfig.getIntProperty("flink.restart.delay", 10000);

        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, restartDelay));

        LOG.info("Environment configured successfully");
    }

    private static DataStream<LabOrder> createLabOrdersStream(StreamExecutionEnvironment env) {
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        String consumerGroup = PropertiesConfig.getProperty("kafka.consumer.group");
        String topic = PropertiesConfig.getProperty("kafka.topics.lab-orders");

        LOG.info("Creating lab orders stream from topic: {}", topic);

        KafkaSource<LabOrder> source = KafkaSource.<LabOrder>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new HL7DeserializationSchema.HL7MessageDeserializationSchema())
                .setProperties(PropertiesConfig.getKafkaProperties())
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
        String transactionPrefix = PropertiesConfig.getProperty("kafka.transaction.lab-orders");

        KafkaSink<LabOrder> jsonSink = KafkaSink.<LabOrder>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<LabOrder>builder()
                        .setTopic(jsonTopic)
                        .setValueSerializationSchema(new JsonSerializationSchemas.LabOrderSerializationSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix(transactionPrefix)
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

    private static DataStream<LabOrderAck> createLabOrderAcksStream(StreamExecutionEnvironment env) {
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        String consumerGroup = PropertiesConfig.getProperty("kafka.consumer.group");
        String topic = PropertiesConfig.getProperty("kafka.topics.lab-orders-ack");

        LOG.info("Creating lab order acknowledgments stream from topic: {}", topic);

        KafkaSource<LabOrderAck> source = KafkaSource.<LabOrderAck>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new HL7DeserializationSchema.HL7AckDeserializationSchema())
                .setProperties(PropertiesConfig.getKafkaProperties())
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
        String transactionPrefix = PropertiesConfig.getProperty("kafka.transaction.lab-orders-ack");

        KafkaSink<LabOrderAck> jsonSink = KafkaSink.<LabOrderAck>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<LabOrderAck>builder()
                        .setTopic(jsonTopic)
                        .setValueSerializationSchema(new JsonSerializationSchemas.LabOrderAckSerializationSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix(transactionPrefix)
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

    private static DataStream<LabResult> createLabResultsStream(StreamExecutionEnvironment env) {
        String bootstrapServers = PropertiesConfig.getProperty("kafka.bootstrap.servers");
        String consumerGroup = PropertiesConfig.getProperty("kafka.consumer.group");
        String topic = PropertiesConfig.getProperty("kafka.topics.lab-results");

        LOG.info("Creating lab results stream from topic: {}", topic);

        KafkaSource<LabResult> source = KafkaSource.<LabResult>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new HL7DeserializationSchema.HL7ResultDeserializationSchema())
                .setProperties(PropertiesConfig.getKafkaProperties())
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
        String transactionPrefix = PropertiesConfig.getProperty("kafka.transaction.lab-results");

        KafkaSink<LabResult> jsonSink = KafkaSink.<LabResult>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<LabResult>builder()
                        .setTopic(jsonTopic)
                        .setValueSerializationSchema(new JsonSerializationSchemas.LabResultSerializationSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix(transactionPrefix)
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