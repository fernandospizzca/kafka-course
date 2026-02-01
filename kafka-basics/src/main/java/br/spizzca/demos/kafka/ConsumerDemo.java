package br.spizzca.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());


    public static void main(String[] args) {
        log.info("I am a Kafka consumer!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create consumer Properties
        Properties properties = new Properties();

        // connect to Localhost
        // properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to Conduktor Playground
        // properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        // properties.setProperty("security.protocol", "SASL_SSL");
        // properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"your-username\" password=\"your-password\";");
        // properties.setProperty("sasl.mechanism", "PLAIN");

        // Connect Aiven Cluster
        // olhar arquivo client para chaves e senhas
        properties.put("bootstrap.servers", "kafka-227b8376-fernando-spizzca.c.aivencloud.com:27517");
        properties.put("security.protocol", "SSL");
        properties.put("ssl.protocol", "TLS");

        // Keystore
        properties.put("ssl.keystore.location", "D:\\OneDrive\\Documentos\\Cursos\\Kafka\\certificados_aiven\\client.keystore.p12");
        properties.put("ssl.keystore.type", "PKCS12");

        // Truststore
        properties.put("ssl.truststore.location", "D:\\OneDrive\\Documentos\\Cursos\\Kafka\\certificados_aiven\\client.truststore.jks");
        properties.put("ssl.truststore.type", "JKS");

        // set Consumer properties
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "earliest");

        // create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for data
        while (true) {

            log.info("Polling");

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record: records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }

        }

    }
}
