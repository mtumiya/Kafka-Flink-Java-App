package zm.gov.moh.hie.pipeline.disa;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresSinkSchemas {
    private static final ObjectMapper objectMapper = BaseJsonSerializationSchema.objectMapper;

    /**
     * PostgreSQL sink for LabOrder objects.
     */
    public static class LabOrderSink extends BasePostgresSink<LabOrder> {
        private static final long serialVersionUID = 1L;
        private static final String INSERT_SQL =
                "INSERT INTO lab_orders " +
                        "(lab_order_id, sending_facility_id, receiving_lab_code, sending_date_time, lab_tests_ordered) " +
                        "VALUES (?, ?, ?, ?, ?::jsonb) " +
                        "ON CONFLICT (lab_order_id) DO UPDATE SET " +
                        "sending_facility_id = EXCLUDED.sending_facility_id, " +
                        "receiving_lab_code = EXCLUDED.receiving_lab_code, " +
                        "sending_date_time = EXCLUDED.sending_date_time, " +
                        "lab_tests_ordered = EXCLUDED.lab_tests_ordered";

        public LabOrderSink(String url, String username, String password) {
            super(url, username, password);
        }

        @Override
        protected String getInsertSql() {
            return INSERT_SQL;
        }

        @Override
        protected void setStatementParameters(PreparedStatement statement, LabOrder labOrder)
                throws SQLException {
            try {
                setString(statement, 1, labOrder.getLabOrderId());
                setString(statement, 2, labOrder.getSendingFacilityId());
                setString(statement, 3, labOrder.getReceivingLabCode());
                setTimestamp(statement, 4, labOrder.getSendingDateTime());

                String labTestsJson = objectMapper.writeValueAsString(labOrder.getLabTestsOrdered());
                setString(statement, 5, labTestsJson);
            } catch (Exception e) {
                throw new SQLException("Failed to set statement parameters", e);
            }
        }
    }

    /**
     * PostgreSQL sink for LabOrderAck objects.
     */
    public static class LabOrderAckSink extends BasePostgresSink<LabOrderAck> {
        private static final long serialVersionUID = 1L;
        private static final String INSERT_SQL =
                "INSERT INTO lab_orders_ack " +
                        "(sending_facility_id, targeted_disa_code, sending_date_time, message_id, " +
                        "ack_message_id, acknowledgment_status, acknowledgment_description) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (ack_message_id) DO UPDATE SET " +
                        "sending_facility_id = EXCLUDED.sending_facility_id, " +
                        "targeted_disa_code = EXCLUDED.targeted_disa_code, " +
                        "sending_date_time = EXCLUDED.sending_date_time, " +
                        "message_id = EXCLUDED.message_id, " +
                        "acknowledgment_status = EXCLUDED.acknowledgment_status, " +
                        "acknowledgment_description = EXCLUDED.acknowledgment_description";

        public LabOrderAckSink(String url, String username, String password) {
            super(url, username, password);
        }

        @Override
        protected String getInsertSql() {
            return INSERT_SQL;
        }

        @Override
        protected void setStatementParameters(PreparedStatement statement, LabOrderAck ack)
                throws SQLException {
            setString(statement, 1, ack.getSendingFacilityId());
            setString(statement, 2, ack.getTargetedDisaCode());
            setTimestamp(statement, 3, ack.getSendingDateTime());
            setString(statement, 4, ack.getMessageId());
            setString(statement, 5, ack.getAckMessageId());
            setString(statement, 6, ack.getAcknowledgmentStatus());
            setString(statement, 7, ack.getAcknowledgmentDescription());
        }
    }

    /**
     * PostgreSQL sink for LabResult objects.
     */
    public static class LabResultSink extends BasePostgresSink<LabResult> {
        private static final long serialVersionUID = 1L;
        private static final String INSERT_SQL =
                "INSERT INTO crt.lab_results " +
                        "(lab_order_id, sending_facility_id, receiving_lab_code, sending_date_time, " +
                        "lab_test_results, message_id, patient_nupn) " +
                        "VALUES (?, ?, ?, ?, ?::jsonb, ?, ?) " +
                        "ON CONFLICT (lab_order_id) DO UPDATE SET " +
                        "sending_facility_id = EXCLUDED.sending_facility_id, " +
                        "receiving_lab_code = EXCLUDED.receiving_lab_code, " +
                        "sending_date_time = EXCLUDED.sending_date_time, " +
                        "lab_test_results = EXCLUDED.lab_test_results, " +
                        "message_id = EXCLUDED.message_id, " +
                        "patient_nupn = EXCLUDED.patient_nupn";

        public LabResultSink(String url, String username, String password) {
            super(url, username, password);
        }

        @Override
        protected String getInsertSql() {
            return INSERT_SQL;
        }

        @Override
        protected void setStatementParameters(PreparedStatement statement, LabResult labResult)
                throws SQLException {
            try {
                statement.setString(1, labResult.getLabOrderId());
                statement.setString(2, labResult.getSendingFacilityId());
                statement.setString(3, labResult.getReceivingLabCode());
                setTimestamp(statement, 4, labResult.getSendingDateTime());

                String resultsJson = objectMapper.writeValueAsString(labResult.getLabTestResults());
                statement.setString(5, resultsJson);

                statement.setString(6, labResult.getMessageId());
                statement.setString(7, labResult.getPatientNupn());
            } catch (Exception e) {
                throw new SQLException("Failed to set statement parameters", e);
            }
        }
    }
}