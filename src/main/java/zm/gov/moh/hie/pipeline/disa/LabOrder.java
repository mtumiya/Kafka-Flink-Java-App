package zm.gov.moh.hie.pipeline.disa;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import java.util.List;

public class LabOrder {
    @JsonProperty("lab_order_id")
    private String labOrderId;

    @JsonProperty("sending_facility_id")
    private String sendingFacilityId;

    @JsonProperty("targeted_disa_code")
    private String targetedDisaCode;

    @JsonProperty("sending_date_time")
    private LocalDateTime sendingDateTime;

    @JsonProperty("lab_tests_ordered")
    private List<LabTest> labTestsOrdered;

    @JsonProperty("message_id")
    private String messageId;

    @JsonProperty("patient")
    private Patient patient;

    // Nested Patient class
    public static class Patient {
        private String nupn;
        private String sex;

        // Getters and setters
        public String getNupn() { return nupn; }
        public void setNupn(String nupn) { this.nupn = nupn; }
        public String getSex() { return sex; }
        public void setSex(String sex) { this.sex = sex; }
    }

    // Nested LabTest class
    public static class LabTest {
        private String code;
        private String name;
        private String type;

        // Getters and setters
        public String getCode() { return code; }
        public void setCode(String code) { this.code = code; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
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

    public List<LabTest> getLabTestsOrdered() { return labTestsOrdered; }
    public void setLabTestsOrdered(List<LabTest> labTestsOrdered) { this.labTestsOrdered = labTestsOrdered; }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public Patient getPatient() { return patient; }
    public void setPatient(Patient patient) { this.patient = patient; }
}