package zm.gov.moh.hie.pipeline.disa;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import java.util.List;

public class LabResult {
    @JsonProperty("lab_order_id")
    private String labOrderId;

    @JsonProperty("sending_facility_id")
    private String sendingFacilityId;

    @JsonProperty("targeted_disa_code")
    private String targetedDisaCode;

    @JsonProperty("sending_date_time")
    private LocalDateTime sendingDateTime;

    @JsonProperty("lab_test_results")
    private List<TestResult> labTestResults;

    @JsonProperty("message_id")
    private String messageId;

    @JsonProperty("patient_nupn")
    private String patientNupn;

    // Nested TestResult class
    public static class TestResult {
        private String code;
        private String name;
        private String value;
        private String unit;
        private String referenceRange;
        private LocalDateTime observationDateTime;

        // Getters and setters
        public String getCode() { return code; }
        public void setCode(String code) { this.code = code; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
        public String getUnit() { return unit; }
        public void setUnit(String unit) { this.unit = unit; }
        public String getReferenceRange() { return referenceRange; }
        public void setReferenceRange(String referenceRange) { this.referenceRange = referenceRange; }
        public LocalDateTime getObservationDateTime() { return observationDateTime; }
        public void setObservationDateTime(LocalDateTime observationDateTime) { this.observationDateTime = observationDateTime; }
    }

    // Getters and setters for main class
    public String getLabOrderId() { return labOrderId; }
    public void setLabOrderId(String labOrderId) { this.labOrderId = labOrderId; }

    public String getSendingFacilityId() { return sendingFacilityId; }
    public void setSendingFacilityId(String sendingFacilityId) { this.sendingFacilityId = sendingFacilityId; }

    public String getTargetedDisaCode() { return targetedDisaCode; }
    public void setTargetedDisaCode(String targetedDisaCode) { this.targetedDisaCode = targetedDisaCode; }

    public LocalDateTime getSendingDateTime() { return sendingDateTime; }
    public void setSendingDateTime(LocalDateTime sendingDateTime) { this.sendingDateTime = sendingDateTime; }

    public List<TestResult> getLabTestResults() { return labTestResults; }
    public void setLabTestResults(List<TestResult> labTestResults) { this.labTestResults = labTestResults; }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public String getPatientNupn() { return patientNupn; }
    public void setPatientNupn(String patientNupn) { this.patientNupn = patientNupn; }
}