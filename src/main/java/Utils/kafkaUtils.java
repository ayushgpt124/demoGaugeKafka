package Utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

public class kafkaUtils {

    Properties props = new Properties();
    String brokers = "glider.srvs.cloudkafka.com:9094";
    String username = "gwiskbew";
    String password = "sWs-KKxV1dflMUMJYBYZ6zAcsRgRtvFk";


    public void createKafkaConnection() {

        props.setProperty("bootstrap.servers", brokers);
        props.put("enable.idempotence", "false");
        //handle auth
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";");

    }

    public void sendMessageToKafka(String topic, String message) {

        //converting objects to bytes
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> myproducer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);

        myproducer.send(record);
        myproducer.flush();
        myproducer.close();
    }

    public void consumeMessageFromTopics(String topic) {
        //converting bytes to objects
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());

        props.setProperty("group.id", "gwiskbew-");
        //consumer object
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //topic to consume from
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("consumeMessageFromTopics-----> "+record.value());
            }
        }

    }

    public void getLatestMessageFromKafkaTopic(String topic) {
        //converting bytes to objects
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "latest");
        props.setProperty("group.id", "gwiskbew-");
        //consumer object
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //topic to consume from
        consumer.subscribe(Arrays.asList(topic));

        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            for (ConsumerRecord<String, String> record : records) {
                String msg = record.value();
                System.out.println("Latest message from topic: " + msg);

                long offset = record.offset();
                System.out.println("offset value is: " + offset);

                if (offset > 0) {
                    flag = false;
                    consumer.close();
                }
            }
        }
    }
}









