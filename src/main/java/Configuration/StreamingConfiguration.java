package Configuration;


import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.Properties;

public class StreamingConfiguration {
    private static Properties getKafkaProperties(String groupId) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "154.120.216.119:9093,102.23.123.251:9093,102.23.120.153:9093");
        props.setProperty("group.id", groupId);
        props.setProperty("security.protocol", "SASL_PLAINTEXT");
        props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"admin\""
                + "password=\"075F80FED7C6\";");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("session.timeout.ms", "30000");
        props.setProperty("heartbeat.interval.ms", "10000");
        props.setProperty("max.poll.interval.ms", "300000");
        props.setProperty("fetch.max.wait.ms", "500");

        return props;
    }
    public static FlinkKafkaConsumer<String> createKafkaConsumer(String topic, String groupId) {
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), getKafkaProperties(groupId));
    }
}
