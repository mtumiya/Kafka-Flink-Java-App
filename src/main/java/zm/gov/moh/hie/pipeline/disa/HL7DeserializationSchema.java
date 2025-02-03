package zm.gov.moh.hie.pipeline.disa;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HL7DeserializationSchema {
    private static final Logger LOG = LoggerFactory.getLogger(HL7DeserializationSchema.class);

    /**
     * Message Deserializer for Lab Orders (OML^O21)
     */
    public static class HL7MessageDeserializationSchema extends BaseHL7DeserializationSchema<LabOrder> {
        @Override
        protected LabOrder parseHL7Message(String message) throws Exception {
            try {
                return HL7MessageParser.parseMessage(message);
            } catch (Exception e) {
                LOG.error("Error parsing HL7 message: {}", e.getMessage(), e);
                throw e;
            }
        }

        @Override
        public TypeInformation<LabOrder> getProducedType() {
            return TypeInformation.of(LabOrder.class);
        }
    }

    /**
     * ACK Deserializer for Lab Order Acknowledgments (ACK)
     */
    public static class HL7AckDeserializationSchema extends BaseHL7DeserializationSchema<LabOrderAck> {
        @Override
        protected LabOrderAck parseHL7Message(String message) throws Exception {
            try {
                return HL7AckMessageParser.parseMessage(message);
            } catch (Exception e) {
                LOG.error("Error parsing HL7 ACK message: {}", e.getMessage(), e);
                throw e;
            }
        }

        @Override
        public TypeInformation<LabOrderAck> getProducedType() {
            return TypeInformation.of(LabOrderAck.class);
        }
    }

    /**
     * Result Deserializer for Lab Results (ORU^R01)
     */
    public static class HL7ResultDeserializationSchema extends BaseHL7DeserializationSchema<LabResult> {
        @Override
        protected LabResult parseHL7Message(String message) throws Exception {
            try {
                return HL7ResultMessageParser.parseMessage(message);
            } catch (Exception e) {
                LOG.error("Error parsing HL7 result message: {}", e.getMessage(), e);
                throw e;
            }
        }

        @Override
        public TypeInformation<LabResult> getProducedType() {
            return TypeInformation.of(LabResult.class);
        }
    }
}