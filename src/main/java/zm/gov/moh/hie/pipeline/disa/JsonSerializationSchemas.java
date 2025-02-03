package zm.gov.moh.hie.pipeline.disa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains serialization schemas for converting different types of messages to JSON format.
 */
public class JsonSerializationSchemas {
    private static final Logger LOG = LoggerFactory.getLogger(JsonSerializationSchemas.class);

    /**
     * Serialization schema for converting LabOrder objects to JSON format.
     */
    public static class LabOrderSerializationSchema extends BaseJsonSerializationSchema<LabOrder> {
        @Override
        public byte[] serialize(LabOrder value) {
            try {
                return super.serialize(value);
            } catch (Exception e) {
                LOG.error("Error serializing LabOrder with ID {}: {}",
                        value != null ? value.getLabOrderId() : "null",
                        e.getMessage(), e);
                throw e;
            }
        }
    }

    /**
     * Serialization schema for converting LabOrderAck objects to JSON format.
     */
    public static class LabOrderAckSerializationSchema extends BaseJsonSerializationSchema<LabOrderAck> {
        @Override
        public byte[] serialize(LabOrderAck value) {
            try {
                return super.serialize(value);
            } catch (Exception e) {
                LOG.error("Error serializing LabOrderAck with Message ID {}: {}",
                        value != null ? value.getMessageId() : "null",
                        e.getMessage(), e);
                throw e;
            }
        }
    }

    /**
     * Serialization schema for converting LabResult objects to JSON format.
     */
    public static class LabResultSerializationSchema extends BaseJsonSerializationSchema<LabResult> {
        @Override
        public byte[] serialize(LabResult value) {
            try {
                return super.serialize(value);
            } catch (Exception e) {
                LOG.error("Error serializing LabResult with Order ID {}: {}",
                        value != null ? value.getLabOrderId() : "null",
                        e.getMessage(), e);
                throw e;
            }
        }
    }
}