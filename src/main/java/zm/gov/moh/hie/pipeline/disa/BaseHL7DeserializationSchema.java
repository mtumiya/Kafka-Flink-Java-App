package zm.gov.moh.hie.pipeline.disa;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public abstract class BaseHL7DeserializationSchema<T> implements DeserializationSchema<T> {
    private static final Logger LOG = LoggerFactory.getLogger(BaseHL7DeserializationSchema.class);

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            LOG.warn("Received null or empty byte array");
            return null;
        }

        try {
            String hl7Message = new String(bytes, StandardCharsets.UTF_8);
            if (hl7Message.trim().isEmpty()) {
                LOG.warn("Received empty HL7 message");
                return null;
            }
            return parseHL7Message(hl7Message);
        } catch (Exception e) {
            LOG.error("Error deserializing HL7 message: {}", e.getMessage(), e);
            throw new IOException("Failed to deserialize HL7 message", e);
        }
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    protected abstract T parseHL7Message(String message) throws Exception;
}