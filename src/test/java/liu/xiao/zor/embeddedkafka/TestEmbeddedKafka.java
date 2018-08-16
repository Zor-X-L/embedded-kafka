package liu.xiao.zor.embeddedkafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestEmbeddedKafka {

    private static final int REPLICATION_FACTOR = 3;

    private static EmbeddedKafka kafka;

    @BeforeClass
    public static void beforeClass() throws Exception {

        Properties serverProps = new Properties();
        serverProps.setProperty("delete.topic.enable", "true");

        kafka = new EmbeddedKafkaBuilder()
                .numServer(REPLICATION_FACTOR)
                .serverProperties(serverProps)
                .build().start();
    }

    @AfterClass
    public static void afterClass() throws Exception {

        kafka.stop();
    }

    @Test
    public void test1() throws Exception {

        kafka.createTopic("test1a", 1, REPLICATION_FACTOR);
        kafka.deleteTopic("test1a");

        try {
            kafka.deleteTopic("test1a");
        } catch (Exception e) {
            // That's ok
        }

        try {
            kafka.deleteTopic("test1b");
        } catch (Exception e) {
            // That's ok
        }
    }

    @Test
    public void test2() {

        kafka.createTopic("test2", 1, REPLICATION_FACTOR);

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", kafka.getBrokerConnectionString());
        producerProps.setProperty("acks", "all");
        producerProps.setProperty("max.in.flight.requests.per.connection", "1");
        KafkaProducer<String, String> producer = new KafkaProducer<>(
                producerProps, new StringSerializer(), new StringSerializer());

        producer.send(new ProducerRecord<>("test2", "r1"));
        producer.send(new ProducerRecord<>("test2", "r2"));
        producer.send(new ProducerRecord<>("test2", "r3"));
        producer.send(new ProducerRecord<>("test2", "r4"));
        producer.send(new ProducerRecord<>("test2", "r5"));
        producer.flush();
        producer.close();

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", kafka.getBrokerConnectionString());
        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("auto.offset.reset", "earliest");
        consumerProps.setProperty("group.id", UUID.randomUUID().toString());

        System.out.print("getAllRecordsFromTopic(): ");
        Collection<ConsumerRecord<String, String>> records = kafka.getAllRecordsFromTopic(
                consumerProps, "test2", 0, 5000);
        System.out.println("Done");

        assertFalse(records.isEmpty());

        Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
        assertEquals("r1", iterator.next().value());
        assertEquals("r2", iterator.next().value());
        assertEquals("r3", iterator.next().value());
        assertEquals("r4", iterator.next().value());
        assertEquals("r5", iterator.next().value());
    }
}
