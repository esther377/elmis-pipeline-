import HelperClass.PrescriptionAckRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProducerJunit {
    private KafkaPrescriptionProducer kafkaPrescriptionProducer;

    @BeforeEach
    void setUp() {
        kafkaPrescriptionProducer = new KafkaPrescriptionProducer();
    }

    @Test
    void testSerializationSchema() {
        KafkaSerializationSchema<PrescriptionAckRecord> schema = new PrescriptionAckSerializationSchema();
        PrescriptionAckRecord record = new PrescriptionAckRecord("906785", "{\"message\": \"test\"}");

        ProducerRecord<byte[], byte[]> producerRecord = schema.serialize(record, System.currentTimeMillis());
        Assertions.assertNotNull(producerRecord);
        Assertions.assertEquals("h-906785678_m-PR", producerRecord.topic());
        Assertions.assertNotNull(producerRecord.value());
    }
}
