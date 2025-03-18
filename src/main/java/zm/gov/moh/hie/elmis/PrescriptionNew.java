package zm.gov.moh.hie.elmis;

import Configuration.DbConfiguration;
import Configuration.StreamingConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class PrescriptionNew {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "prescriptions";
        String groupId = "hie-manager-stream-prescription-group-new12367";
        FlinkKafkaConsumer<String> kafkaConsumer = StreamingConfiguration.createKafkaConsumer(topic,groupId);
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);
        DataStream<PrescriptionRecord> prescriptionStream = kafkaStream.map(new MapFunction<String, PrescriptionRecord>() {
            private final ObjectMapper objectMapper = new ObjectMapper();
            @Override
            public PrescriptionRecord map(String value) throws Exception {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                JsonNode payload = objectMapper.readTree(value);
                JsonNode msh = payload.path("msh");
                JsonNode regimen = payload.path("regimen");
                JsonNode prescription = payload.path("prescription");
                String timestampStr = msh.path("timestamp").asText();
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                Timestamp timestamp = Timestamp.valueOf(localDateTime);

                return new PrescriptionRecord(
                        timestamp,
                        msh.path("sendingApplication").asText(),
                        msh.path("receivingApplication").asText(),
                        msh.path("messageId").asText(),
                        msh.path("hmisCode").asText(),
                        regimen.path("regimenCode").asText(),
                        regimen.path("duration").asInt(),
                        prescription.path("prescriptionDrugs").size(),
                        payload.path("prescriptionUuid").asText()
                );
            }
        });

        prescriptionStream.addSink(JdbcSink.sink(
                "INSERT INTO prescription_new (timestamp, sending_application, receiving_application, message_id, hmis_code, regimen_code, regimen_duration, prescriptions_count, prescription_uuid) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (statement, record) -> {
                    statement.setTimestamp(1, record.timestamp);
                    statement.setString(2, record.sendingApplication);
                    statement.setString(3, record.receivingApplication);
                    statement.setString(4, record.messageId);
                    statement.setString(5, record.hmisCode);
                    statement.setString(6, record.regimenCode);
                    statement.setInt(7, record.regimenDuration);
                    statement.setInt(8, record.prescriptionsCount);
                    statement.setObject(9, java.util.UUID.fromString(record.prescriptionUuid));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                DbConfiguration.getConnectionOptions()
        ));

        env.execute("Prescription Flink Job");
    }

    public static class PrescriptionRecord {
        public Timestamp timestamp;
        public String sendingApplication;
        public String receivingApplication;
        public String messageId;
        public String hmisCode;
        public String regimenCode;
        public int regimenDuration;
        public int prescriptionsCount;
        public String prescriptionUuid;

        public PrescriptionRecord(Timestamp timestamp, String sendingApplication, String receivingApplication, String messageId, String hmisCode, String regimenCode, int regimenDuration, int prescriptionsCount, String prescriptionUuid) {
            this.timestamp = timestamp;
            this.sendingApplication = sendingApplication;
            this.receivingApplication = receivingApplication;
            this.messageId = messageId;
            this.hmisCode = hmisCode;
            this.regimenCode = regimenCode;
            this.regimenDuration = regimenDuration;
            this.prescriptionsCount = prescriptionsCount;
            this.prescriptionUuid = prescriptionUuid;
        }
    }



}




