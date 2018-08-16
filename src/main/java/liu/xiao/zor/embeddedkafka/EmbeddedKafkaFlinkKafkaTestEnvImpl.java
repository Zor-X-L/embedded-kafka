package liu.xiao.zor.embeddedkafka;

import org.apache.flink.streaming.connectors.kafka.KafkaTestEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;
import java.util.Properties;

public class EmbeddedKafkaFlinkKafkaTestEnvImpl implements EmbeddedKafka {

    private int numServer;
    private Properties serverProperties;

    private KafkaTestEnvironment kafka;
    KafkaTestEnvironment.Config kafkaConfig;

    EmbeddedKafkaFlinkKafkaTestEnvImpl(EmbeddedKafkaBuilder builder)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {

        numServer = builder.getNumServer();
        serverProperties = builder.getServerProperties();
        Class<?> clazz = Class.forName("org.apache.flink.streaming.connectors.kafka.KafkaTestEnvironmentImpl");
        kafka = (KafkaTestEnvironment) clazz.newInstance();
        kafkaConfig = KafkaTestEnvironment.createConfig()
                .setKafkaServersNumber(numServer)
                .setSecureMode(false)
                .setHideKafkaBehindProxy(false)
                .setKafkaServerProperties(serverProperties);
    }

    @Override
    public EmbeddedKafka start() {
        kafka.prepare(kafkaConfig);
        return this;
    }

    @Override
    public void stop() throws Exception {
        kafka.shutdown();
    }

    @Override
    public String getBrokerConnectionString() {
        return kafka.getBrokerConnectionString();
    }

    @Override
    public EmbeddedKafka createTopic(String topic, int numPartition, int replicationFactor) {
        kafka.createTestTopic(topic, numPartition, replicationFactor);
        return this;
    }

    @Override
    public EmbeddedKafka deleteTopic(String topic) {
        kafka.deleteTestTopic(topic);
        return this;
    }

    @Override
    public <K, V> Collection<ConsumerRecord<K, V>> getAllRecordsFromTopic(Properties properties, String topic, int partition, long timeout) {
        return kafka.getAllRecordsFromTopic(properties, topic, partition, timeout);
    }
}
