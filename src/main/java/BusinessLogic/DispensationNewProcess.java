package BusinessLogic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import Configuration.DbConfiguration;
import Configuration.StreamingConfiguration;

public class DispensationNewProcess {
    public static void processDispensationPayloads(StreamExecutionEnvironment env) {
        String groupId = "hie-manager-stream-dispensation-group-new1234";
        FlinkKafkaConsumer<String> kafkaConsumer = StreamingConfiguration.createKafkaConsumer("dispensations", groupId);
        DataStream<DispensationRecord> dispensationStream = env
                .addSource(kafkaConsumer)
                .map(new MapFunction<String, DispensationRecord>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();
                    @Override
                    public DispensationRecord map(String json) throws Exception {
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        JsonNode payload;
                        try {
                            payload = objectMapper.readTree(json);
                        } catch (Exception e) {
                            System.err.println("Failed to parse JSON: " + e.getMessage());
                            return null;
                        }
                        JsonNode msh = payload.path("msh");
                        JsonNode regimen = payload.path("regimen");
                        String timestampStr = msh.path("timestamp").asText();
                        Timestamp timestamp;
                        if (timestampStr == null || timestampStr.trim().isEmpty()) {
                            System.err.println("Missing or empty timestamp. Using current timestamp.");
                            timestamp = Timestamp.valueOf(LocalDateTime.now());
                        } else {
                            try {
                                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                                timestamp = Timestamp.valueOf(localDateTime);
                            } catch (Exception e) {
                                System.err.println("Invalid timestamp format: " + timestampStr + ". Using current timestamp.");
                                timestamp = Timestamp.valueOf(LocalDateTime.now());
                            }
                        }
                        return new DispensationRecord(
                                timestamp,
                                msh.path("sendingApplication").asText(),
                                msh.path("receivingApplication").asText(),
                                msh.path("messageId").asText(),
                                msh.path("hmisCode").asText(),
                                regimen.path("regimenCode").asText(),
                                regimen.path("duration").asInt(),
                                payload.path("dispensedDrugs").size()
                        );
                    }
                })
                .filter(record -> record != null);
        dispensationStream.addSink(JdbcSink.sink(
                "INSERT INTO dispensation (timestamp, sending_application, receiving_application, message_id, hmis_code, regimen_code, regimen_duration, dispensation_count) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (message_id) DO UPDATE SET " +
                        "timestamp = EXCLUDED.timestamp, " +
                        "sending_application = EXCLUDED.sending_application, " +
                        "receiving_application = EXCLUDED.receiving_application, " +
                        "hmis_code = EXCLUDED.hmis_code, " +
                        "regimen_code = EXCLUDED.regimen_code, " +
                        "regimen_duration = EXCLUDED.regimen_duration, " +
                        "dispensation_count = EXCLUDED.dispensation_count",
                (PreparedStatement statement, DispensationRecord record) -> {
                    statement.setTimestamp(1, record.timestamp);
                    statement.setString(2, record.sendingApplication);
                    statement.setString(3, record.receivingApplication);
                    statement.setString(4, record.messageId);
                    statement.setString(5, record.hmisCode);
                    statement.setString(6, record.regimenCode);
                    statement.setInt(7, record.regimenDuration);
                    statement.setInt(8, record.dispensationsCount);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(500)
                        .withBatchIntervalMs(100)
                        .withMaxRetries(5)
                        .build(),
                DbConfiguration.getConnectionOptions()
        ));

    }
    public static class DispensationRecord {
        public Timestamp timestamp;
        public String sendingApplication;
        public String receivingApplication;
        public String messageId;
        public String hmisCode;
        public String regimenCode;
        public int regimenDuration;
        public int dispensationsCount;
        public DispensationRecord(Timestamp timestamp, String sendingApplication, String receivingApplication,
                                  String messageId, String hmisCode, String regimenCode, int regimenDuration,
                                  int dispensationsCount) {
            this.timestamp = timestamp;
            this.sendingApplication = sendingApplication;
            this.receivingApplication = receivingApplication;
            this.messageId = messageId;
            this.hmisCode = hmisCode;
            this.regimenCode = regimenCode;
            this.regimenDuration = regimenDuration;
            this.dispensationsCount = dispensationsCount;
        }
    }
}
