package zm.gov.moh.hie.elmis;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import HelperClass.PrescriptionAckRecord;
import Configuration.KafkaProducerService; 
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import Configuration.StreamingConfiguration;
public class PrescriptionExtractsInitial {
    public static void main(String[] args) throws Exception {
        System.out.println("Processing.......");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inputTopic = "prescriptions-ack";
        String groupId = "hie-manager-stream-prescription-group-new1234";
        FlinkKafkaConsumer<String> kafkaConsumer = StreamingConfiguration.createKafkaConsumer(inputTopic, groupId);
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);
        DataStream<PrescriptionAckRecord> prescriptionAckStream = kafkaStream.map(new MapFunction<String, PrescriptionAckRecord>() {
            private final ObjectMapper objectMapper = new ObjectMapper();
            @Override
            public PrescriptionAckRecord map(String value) throws Exception {
                JsonNode payload = objectMapper.readTree(value);
                JsonNode msh = payload.path("msh");
                String hmisCode = msh.path("hmisCode").asText();
                return new PrescriptionAckRecord(hmisCode, value);
            }
        });
        prescriptionAckStream.addSink(KafkaProducerService.createKafkaProducer());

        env.execute("Prescription-Ack Router Job");
    }
}
