import HelperClass.PrescriptionAckRecord;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import javax.annotation.Nullable;

public class PrescriptionAckSerializationSchema implements KafkaSerializationSchema<PrescriptionAckRecord> {
    @Override
    public ProducerRecord<byte[], byte[]> serialize(PrescriptionAckRecord record, @Nullable Long timestamp) {
        String topic = "h-" + record.hmisCode + "_m-PR";
        return new ProducerRecord<>(topic, record.payload.getBytes());
    }
}
