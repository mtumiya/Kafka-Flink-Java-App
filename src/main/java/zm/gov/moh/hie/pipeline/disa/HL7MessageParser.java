package zm.gov.moh.hie.pipeline.disa;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v25.message.OML_O21;
import ca.uhn.hl7v2.model.v25.segment.MSH;
import ca.uhn.hl7v2.model.v25.segment.OBR;
import ca.uhn.hl7v2.model.v25.segment.PID;
import ca.uhn.hl7v2.parser.Parser;
import ca.uhn.hl7v2.validation.impl.NoValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class HL7MessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(HL7MessageParser.class);
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

    public static LabOrder parseMessage(String messageText) throws HL7Exception {
        try {
            Message hapiMsg = parser.parse(messageText);
            if (!(hapiMsg instanceof OML_O21)) {
                throw new HL7Exception("Message is not an OML_O21 message");
            }

            OML_O21 omlMessage = (OML_O21) hapiMsg;
            LabOrder labOrder = new LabOrder();

            // Parse MSH segment
            MSH msh = omlMessage.getMSH();
            labOrder.setMessageId(msh.getMessageControlID().getValue());

            // Get the sending facility code (second component of MSH-4)
            String facilityCode = msh.getSendingFacility().getUniversalID().getValue();
            if (facilityCode != null && !facilityCode.isEmpty()) {
                labOrder.setSendingFacilityId(facilityCode);
            } else {
                // Fallback to namespace ID if universal ID is not available
                labOrder.setSendingFacilityId(msh.getSendingFacility().getNamespaceID().getValue());
            }

            // Get date/time from MSH segment
            String dateTimeStr = msh.getDateTimeOfMessage().getTime().getValue();
            labOrder.setSendingDateTime(parseDateTime(dateTimeStr));

            // Parse PID segment
            PID pid = omlMessage.getPATIENT().getPID();
            LabOrder.Patient patient = new LabOrder.Patient();
            patient.setNupn(pid.getPatientIdentifierList(0).getIDNumber().getValue());
            patient.setSex(pid.getAdministrativeSex().getValue());
            labOrder.setPatient(patient);

            // Parse OBR segments for lab tests
            List<LabOrder.LabTest> labTests = new ArrayList<>();

            // Get the lab order ID from the first ORC segment
            labOrder.setLabOrderId(omlMessage.getORDER().getORC().getPlacerOrderNumber().getEntityIdentifier().getValue());

            // Extract OBR segments
            var order = omlMessage.getORDER();
            var observationRequest = order.getOBSERVATION_REQUEST();
            OBR obr = observationRequest.getOBR();

            // Create lab test from OBR
            LabOrder.LabTest labTest = new LabOrder.LabTest();
            var universalService = obr.getUniversalServiceIdentifier();
            labTest.setCode(universalService.getIdentifier().getValue());
            labTest.setName(universalService.getText().getValue());
            labTest.setType("Regular"); // Default type

            labTests.add(labTest);
            labOrder.setLabTestsOrdered(labTests);

            // Extract receiving lab code
            labOrder.setReceivingLabCode(extractReceivingLabCode(omlMessage));

            return labOrder;

        } catch (HL7Exception e) {
            LOG.error("Error parsing HL7 message: " + e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.error("Unexpected error parsing HL7 message: " + e.getMessage(), e);
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

    private static String extractReceivingLabCode(OML_O21 message) throws HL7Exception {
        return message.getMSH().getReceivingFacility().getNamespaceID().getValue();
    }
}