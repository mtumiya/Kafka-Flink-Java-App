package zm.gov.moh.hie.pipeline.disa;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public class LabOrderAck {
    @JsonProperty("sending_facility_id")
    private String sendingFacilityId;

    @JsonProperty("targeted_disa_code")
    private String targetedDisaCode;

    @JsonProperty("sending_date_time")
    private LocalDateTime sendingDateTime;

    @JsonProperty("message_id")
    private String messageId;

    @JsonProperty("ack_message_id")
    private String ackMessageId;

    @JsonProperty("acknowledgment_status")
    private String acknowledgmentStatus;

    @JsonProperty("acknowledgment_description")
    private String acknowledgmentDescription;

    // Getters and setters
    public String getSendingFacilityId() { return sendingFacilityId; }
    public void setSendingFacilityId(String sendingFacilityId) { this.sendingFacilityId = sendingFacilityId; }

    public String getTargetedDisaCode() { return targetedDisaCode; }
    public void setTargetedDisaCode(String targetedDisaCode) { this.targetedDisaCode = targetedDisaCode; }

    public LocalDateTime getSendingDateTime() { return sendingDateTime; }
    public void setSendingDateTime(LocalDateTime sendingDateTime) { this.sendingDateTime = sendingDateTime; }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public String getAckMessageId() { return ackMessageId; }
    public void setAckMessageId(String ackMessageId) { this.ackMessageId = ackMessageId; }

    public String getAcknowledgmentStatus() { return acknowledgmentStatus; }
    public void setAcknowledgmentStatus(String acknowledgmentStatus) { this.acknowledgmentStatus = acknowledgmentStatus; }

    public String getAcknowledgmentDescription() { return acknowledgmentDescription; }
    public void setAcknowledgmentDescription(String acknowledgmentDescription) { this.acknowledgmentDescription = acknowledgmentDescription; }
}