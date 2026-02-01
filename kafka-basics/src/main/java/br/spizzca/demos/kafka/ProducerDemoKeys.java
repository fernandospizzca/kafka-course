package br.spizzca.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());


    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to Localhost
        // properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to Conduktor Playground
        // properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        // properties.setProperty("security.protocol", "SASL_SSL");
        // properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"your-username\" password=\"your-password\";");
        // properties.setProperty("sasl.mechanism", "PLAIN");

        // Connect Aiven Cluster
        properties.put("bootstrap.servers", "kafka-227b8376-fernando-spizzca.c.aivencloud.com:27517");
        properties.put("security.protocol", "SSL");
        properties.put("ssl.protocol", "TLS");

        properties.put("ssl.key.p", ""); //inserir .password AVNS_QT5n5bcsLRmCv206GLP

        // Keystore
        properties.put("ssl.keystore.location", "D:\\OneDrive\\Documentos\\Cursos\\Kafka\\certificados_aiven\\client.keystore.p12");
        properties.put("ssl.keystore.p", ""); //inserir .password AVNS_QT5n5bcsLRmCv206GLP
        properties.put("ssl.keystore.type", "PKCS12");

        // Truststore
        properties.put("ssl.truststore.location", "D:\\OneDrive\\Documentos\\Cursos\\Kafka\\certificados_aiven\\client.truststore.jks");
        properties.put("ssl.truststore.p", ""); //inserir .password AVNS_QT5n5bcsLRmCv206GLP
        properties.put("ssl.truststore.type", "JKS");

        // set producer properties
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j=0; j<2; j++) {


            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world " + i;

                // create a Producer Record
                ProducerRecord<String, String> producerRecord =
//                        new ProducerRecord<>("demo_java", "teste", "hello world");
                        new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Key: " + key + " | Partition: " + metadata.partition());

                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
