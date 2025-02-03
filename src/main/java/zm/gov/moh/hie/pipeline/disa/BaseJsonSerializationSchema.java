package zm.gov.moh.hie.pipeline.disa;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseJsonSerializationSchema<T> implements SerializationSchema<T> {
    private static final Logger LOG = LoggerFactory.getLogger(BaseJsonSerializationSchema.class);
    protected static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID);
    }

    @Override
    public byte[] serialize(T value) {
        if (value == null) {
            LOG.warn("Received null value for serialization");
            return new byte[0];
        }

        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (Exception e) {
            LOG.error("Error serializing to JSON: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to serialize to JSON", e);
        }
    }
}