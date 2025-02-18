package zm.gov.moh.hie.pipeline.disa;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v25.message.ACK;
import ca.uhn.hl7v2.model.v25.segment.MSA;
import ca.uhn.hl7v2.model.v25.segment.MSH;
import ca.uhn.hl7v2.parser.Parser;
import ca.uhn.hl7v2.validation.impl.NoValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class HL7AckMessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(HL7AckMessageParser.class);
    private static final HapiContext hapiContext;
    private static final Parser parser;

    static {
        hapiContext = new DefaultHapiContext();
        // Disable strict validation
        hapiContext.setValidationContext(new NoValidation());
        hapiContext.getParserConfiguration().setValidating(false);
        hapiContext.getParserConfiguration().setAllowUnknownVersions(true);
        parser = hapiContext.getGenericParser();
    }

    public static LabOrderAck parseMessage(String messageText) throws HL7Exception {
        try {
            Message hapiMsg = parser.parse(messageText);
            if (!(hapiMsg instanceof ACK)) {
                throw new HL7Exception("Message is not an ACK message");
            }

            ACK ackMessage = (ACK) hapiMsg;
            LabOrderAck labOrderAck = new LabOrderAck();

            // Parse MSH segment
            MSH msh = ackMessage.getMSH();
            labOrderAck.setSendingFacilityId(msh.getSendingFacility().getNamespaceID().getValue());
            labOrderAck.setTargetedDisaCode(msh.getReceivingApplication().getNamespaceID().getValue());
            labOrderAck.setMessageId(msh.getMessageControlID().getValue());

            // Parse date/time
            String dateTimeStr = msh.getDateTimeOfMessage().getTime().getValue();
            labOrderAck.setSendingDateTime(parseDateTime(dateTimeStr));

            // Parse MSA segment
            MSA msa = ackMessage.getMSA();
            labOrderAck.setAcknowledgmentStatus(msa.getAcknowledgmentCode().getValue());
            labOrderAck.setAckMessageId(msa.getMessageControlID().getValue());
            labOrderAck.setAcknowledgmentDescription(msa.getTextMessage().getValue());

            return labOrderAck;

        } catch (HL7Exception e) {
            LOG.error("Error parsing HL7 ACK message: " + e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.error("Unexpected error parsing HL7 ACK message: " + e.getMessage(), e);
            throw new HL7Exception(e);
        }
    }

    private static LocalDateTime parseDateTime(String hl7DateTime) {
        if (hl7DateTime == null || hl7DateTime.isEmpty()) {
            return null;
        }

        try {
            // Handle different date formats
            String normalizedDateTime = hl7DateTime.replaceAll("[^0-9]", "");

            switch (normalizedDateTime.length()) {
                case 8: // YYYYMMDD
                    return LocalDateTime.parse(normalizedDateTime + "000000",
                            DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                case 12: // YYYYMMDDHHmm
                    return LocalDateTime.parse(normalizedDateTime,
                            DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
                case 14: // YYYYMMDDHHmmss
                    return LocalDateTime.parse(normalizedDateTime,
                            DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                default:
                    LOG.warn("Unexpected datetime format: {}", hl7DateTime);
                    // Pad with zeros if needed
                    if (normalizedDateTime.length() < 14) {
                        normalizedDateTime = normalizedDateTime + "0".repeat(14 - normalizedDateTime.length());
                    }
                    return LocalDateTime.parse(normalizedDateTime.substring(0, 14),
                            DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            }
        } catch (Exception e) {
            LOG.error("Error parsing datetime {}: {}", hl7DateTime, e.getMessage());
            throw e;
        }
    }
}