package liu.xiao.zor.embeddedkafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;
import java.util.Properties;

public interface EmbeddedKafka {

    EmbeddedKafka start();
    void stop() throws Exception;

    String getBrokerConnectionString();

    EmbeddedKafka createTopic(String topic, int numPartition, int replicationFactor);
    EmbeddedKafka deleteTopic(String topic);

    <K, V> Collection<ConsumerRecord<K, V>> getAllRecordsFromTopic(Properties properties,
                                                                   String topic, int partition, long timeout);
}
