package zm.gov.moh.hie.pipeline.disa;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v25.message.ORU_R01;
import ca.uhn.hl7v2.model.v25.segment.MSH;
import ca.uhn.hl7v2.model.v25.segment.OBR;
import ca.uhn.hl7v2.model.v25.segment.OBX;
import ca.uhn.hl7v2.model.v25.segment.PID;
import ca.uhn.hl7v2.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class HL7ResultMessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(HL7ResultMessageParser.class);
    private static final HapiContext hapiContext = new DefaultHapiContext();
    private static final Parser parser = hapiContext.getGenericParser();

    public static LabResult parseMessage(String messageText) throws HL7Exception {
        try {
            Message hapiMsg = parser.parse(messageText);
            if (!(hapiMsg instanceof ORU_R01)) {
                throw new HL7Exception("Message is not an ORU_R01 message");
            }

            ORU_R01 oruMessage = (ORU_R01) hapiMsg;
            LabResult labResult = new LabResult();

            // Parse MSH segment
            MSH msh = oruMessage.getMSH();
            labResult.setMessageId(msh.getMessageControlID().getValue());
            labResult.setSendingFacilityId(msh.getSendingFacility().getNamespaceID().getValue());
            labResult.setTargetedDisaCode(msh.getReceivingApplication().getNamespaceID().getValue());

            // Get date/time from MSH segment
            String dateTimeStr = msh.getDateTimeOfMessage().getTime().getValue();
            labResult.setSendingDateTime(parseDateTime(dateTimeStr));

            // Parse PID segment for NUPN
            PID pid = oruMessage.getPATIENT_RESULT().getPATIENT().getPID();
            labResult.setPatientNupn(pid.getPatientIdentifierList(0).getIDNumber().getValue());

            // Get the lab order ID from ORC segment
            labResult.setLabOrderId(oruMessage.getPATIENT_RESULT().getORDER_OBSERVATION().getORC()
                    .getPlacerOrderNumber().getEntityIdentifier().getValue());

            // Parse OBR and OBX segments for results
            List<LabResult.TestResult> testResults = new ArrayList<>();
            var orderObs = oruMessage.getPATIENT_RESULT().getORDER_OBSERVATION();

            // Get the OBR segment
            OBR obr = orderObs.getOBR();
            String testCode = obr.getUniversalServiceIdentifier().getIdentifier().getValue();
            String testName = obr.getUniversalServiceIdentifier().getText().getValue();

            // Process OBX segments
            int obsCount = orderObs.getOBSERVATIONReps();
            for (int i = 0; i < obsCount; i++) {
                OBX obx = orderObs.getOBSERVATION(i).getOBX();
                LabResult.TestResult result = new LabResult.TestResult();
                result.setCode(testCode);
                result.setName(testName);

                // Only process if we have values
                if (obx.getObservationValue().length > 0) {
                    result.setValue(obx.getObservationValue(0).encode());
                    result.setUnit(obx.getUnits().getIdentifier().getValue());
                    result.setReferenceRange(obx.getReferencesRange().getValue());

                    // Parse observation datetime
                    String obsDateTime = obx.getDateTimeOfTheObservation().getTime().getValue();
                    if (obsDateTime != null && !obsDateTime.isEmpty()) {
                        result.setObservationDateTime(parseDateTime(obsDateTime));
                    }

                    testResults.add(result);
                }
            }

            labResult.setLabTestResults(testResults);
            return labResult;

        } catch (HL7Exception e) {
            LOG.error("Error parsing HL7 result message: " + e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.error("Unexpected error parsing HL7 result message: " + e.getMessage(), e);
            throw new HL7Exception(e);
        }
    }

    private static LocalDateTime parseDateTime(String hl7DateTime) {
        if (hl7DateTime == null || hl7DateTime.isEmpty()) {
            return null;
        }

        // Handle different HL7 datetime formats
        try {
            switch (hl7DateTime.length()) {
                case 12: // YYYYMMDDHHmm
                    return LocalDateTime.parse(hl7DateTime, DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
                case 14: // YYYYMMDDHHmmss
                    return LocalDateTime.parse(hl7DateTime, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                case 19: // YYYYMMDDHHmmss.SSS
                    return LocalDateTime.parse(hl7DateTime, DateTimeFormatter.ofPattern("yyyyMMddHHmmss.SSS"));
                default:
                    LOG.warn("Unexpected datetime format: {}", hl7DateTime);
                    // Try to parse with base format by padding with zeros if needed
                    String paddedDateTime = hl7DateTime + "00".repeat((14 - hl7DateTime.length()) / 2);
                    return LocalDateTime.parse(paddedDateTime, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            }
        } catch (Exception e) {
            LOG.error("Error parsing datetime {}: {}", hl7DateTime, e.getMessage());
            throw e;
        }
    }
}