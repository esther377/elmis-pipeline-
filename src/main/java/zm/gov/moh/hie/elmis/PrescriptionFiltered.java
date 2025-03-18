package zm.gov.moh.hie.elmis;
import Configuration.KafkaProducerService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import Configuration.StreamingConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import HelperClass.PrescriptionAckRecord;
import Configuration.StreamingConfiguration;
public class PrescriptionFiltered {
    public static void main(String[] args) throws Exception {
        System.out.println("Processing.......");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "prescriptions";
        String groupId = "hie-manager-stream-prescription-group-new1234";
        FlinkKafkaConsumer<String> kafkaConsumer = StreamingConfiguration.createKafkaConsumer(topic, groupId);
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
        env.execute("Prescription-Filtered Payload Router Job");

    }
}
