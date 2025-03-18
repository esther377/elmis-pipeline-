import HelperClass.PrescriptionAckRecord;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.Properties;

public class KafkaPrescriptionProducer {
    private final Properties producerProperties;

    public KafkaPrescriptionProducer() {
        producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "154.120.216.119:9093,102.23.123.251:9093,102.23.120.153:9093");
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProperties.setProperty("security.protocol", "SASL_PLAINTEXT");
        producerProperties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        producerProperties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"admin\" "
                + "password=\"075F80FED7C6\";");
    }

    public FlinkKafkaProducer<PrescriptionAckRecord> createProducer() {
        return new FlinkKafkaProducer<>(
                "default-topic",
                new PrescriptionAckSerializationSchema(),
                producerProperties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
