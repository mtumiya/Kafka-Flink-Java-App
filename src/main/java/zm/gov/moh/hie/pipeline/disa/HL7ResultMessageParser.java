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

            // Get the sending facility code
            String facilityCode = msh.getSendingFacility().getUniversalID().getValue();
            if (facilityCode != null && !facilityCode.isEmpty()) {
                labResult.setSendingFacilityId(facilityCode);
            } else {
                labResult.setSendingFacilityId(msh.getSendingFacility().getNamespaceID().getValue());
            }

            // Get receiving lab code
            labResult.setReceivingLabCode(msh.getReceivingFacility().getNamespaceID().getValue());

            // Get date/time from MSH segment
            String dateTimeStr = msh.getDateTimeOfMessage().getTime().getValue();
            labResult.setSendingDateTime(parseDateTime(dateTimeStr));

            // Parse PID segment for NUPN
            PID pid = oruMessage.getPATIENT_RESULT().getPATIENT().getPID();
            labResult.setPatientNupn(pid.getPatientIdentifierList(0).getIDNumber().getValue());

            // Get the lab order ID
            labResult.setLabOrderId(oruMessage.getPATIENT_RESULT().getORDER_OBSERVATION().getORC()
                    .getPlacerOrderNumber().getEntityIdentifier().getValue());

            // Create a single combined test result
            var orderObs = oruMessage.getPATIENT_RESULT().getORDER_OBSERVATION();
            OBR obr = orderObs.getOBR();

            List<LabResult.TestResult> testResults = new ArrayList<>();
            LabResult.TestResult result = new LabResult.TestResult();

            // Set test info from OBR
            result.setCode(obr.getUniversalServiceIdentifier().getIdentifier().getValue());
            result.setName(obr.getUniversalServiceIdentifier().getText().getValue());

            // Find the first OBX with actual results
            boolean foundResult = false;
            for (int i = 0; i < orderObs.getOBSERVATIONReps() && !foundResult; i++) {
                OBX obx = orderObs.getOBSERVATION(i).getOBX();

                // Skip empty or header OBX segments
                if (obx.getObservationValue().length == 0) {
                    continue;
                }

                String value = obx.getObservationValue(0).encode();
                if (value != null && !value.trim().isEmpty()) {
                    result.setValue(value);
                    result.setUnit(obx.getUnits().getIdentifier().getValue());
                    result.setReferenceRange(obx.getReferencesRange().getValue());

                    // Set observation datetime
                    String obsDateTime = obx.getDateTimeOfTheObservation().getTime().getValue();
                    if (obsDateTime != null && !obsDateTime.isEmpty()) {
                        result.setObservationDateTime(parseDateTime(obsDateTime));
                    }

                    foundResult = true;
                }
            }

            // Only add if we found a valid result
            if (foundResult) {
                testResults.add(result);
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

        try {
            switch (hl7DateTime.length()) {
                case 8: // YYYYMMDD
                    return LocalDateTime.parse(hl7DateTime + "000000",
                            DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                case 12: // YYYYMMDDHHmm
                    return LocalDateTime.parse(hl7DateTime,
                            DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
                case 14: // YYYYMMDDHHmmss
                    return LocalDateTime.parse(hl7DateTime,
                            DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                default:
                    LOG.warn("Unexpected datetime format: {}", hl7DateTime);
                    // Try to parse with base format by padding with zeros if needed
                    String paddedDateTime = hl7DateTime + "00".repeat((14 - hl7DateTime.length()) / 2);
                    return LocalDateTime.parse(paddedDateTime,
                            DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            }
        } catch (Exception e) {
            LOG.error("Error parsing datetime {}: {}", hl7DateTime, e.getMessage());
            throw e;
        }
    }
}