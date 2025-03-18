package BusinessLogic;

import Configuration.DbConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import Configuration.StreamingConfiguration;

public class PrescriptionAckProcess {
    public static void processAcks(StreamExecutionEnvironment env) throws Exception {
        String  groupId = "";
        FlinkKafkaConsumer<String> kafkaConsumer = StreamingConfiguration.createKafkaConsumer("prescriptions-ack","hie-manager-stream-p_ack-group-new1234");
        DataStream<Prescription_Ack> prescriptionAckStream = env
                .addSource(kafkaConsumer)
                .map(new MapFunction<String, Prescription_Ack>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();
                    @Override
                    public Prescription_Ack map(String json) throws Exception {
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        JsonNode payload;
                        try {
                            payload = objectMapper.readTree(json);
                        } catch (Exception e) {
                            System.err.println("Failed to parse JSON: " + e.getMessage());
                            return null;
                        }
                        JsonNode msh = payload.path("msh");
                        String timestampStr = msh.path("timestamp").asText(null);
                        Timestamp timestamp = null;
                        if (timestampStr != null && !timestampStr.isEmpty()) {
                            LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                            timestamp = Timestamp.valueOf(localDateTime);
                        }
                        String ackCode = payload.path("ackCode").asText(null);
                        String refMessageIdStr = payload.path("refMessageId").asText(null);
                        UUID refMessageId = (refMessageIdStr != null && !refMessageIdStr.isEmpty()) ? UUID.fromString(refMessageIdStr) : null;

                        return new Prescription_Ack(
                                timestamp,
                                msh.path("sendingApplication").asText(),
                                msh.path("receivingApplication").asText(),
                                msh.path("messageId").asText(),
                                ackCode,
                                refMessageId
                        );
                    }
                })
                .filter(record -> record != null);

        prescriptionAckStream.addSink(JdbcSink.sink(
                "INSERT INTO prescription_ack (timestamp, sending_application, receiving_application, message_id, acknowledgement_code, referenced_message_id) " +
                        "VALUES (?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (message_id) DO UPDATE SET " +
                        "timestamp = EXCLUDED.timestamp, " +
                        "sending_application = EXCLUDED.sending_application, " +
                        "receiving_application = EXCLUDED.receiving_application, " +
                        "acknowledgement_code = EXCLUDED.acknowledgement_code, " +
                        "referenced_message_id = EXCLUDED.referenced_message_id",

                (PreparedStatement statement, Prescription_Ack record) -> {
                    statement.setTimestamp(1, record.timestamp);
                    statement.setString(2, record.sendingApplication);
                    statement.setString(3, record.receivingApplication);
                    statement.setString(4, record.messageId);
                    statement.setString(5, record.ackCode);
                    if (record.refMsgId != null) {
                        statement.setString(6, record.refMsgId.toString());
                    } else {
                        statement.setNull(6, java.sql.Types.VARCHAR);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                DbConfiguration.getConnectionOptions()

        ));
    }

    public static class Prescription_Ack {
        public Timestamp timestamp;
        public String sendingApplication;
        public String receivingApplication;
        public String messageId;
        public String ackCode;
        public UUID refMsgId;

        public Prescription_Ack(Timestamp timestamp, String sendingApplication, String receivingApplication,
                                String messageId, String ackCode, UUID refMsgId) {
            this.timestamp = timestamp;
            this.sendingApplication = sendingApplication;
            this.receivingApplication = receivingApplication;
            this.messageId = messageId;
            this.ackCode = ackCode;
            this.refMsgId = refMsgId;
        }
    }


}
