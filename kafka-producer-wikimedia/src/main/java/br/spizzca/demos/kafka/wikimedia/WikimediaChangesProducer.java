package br.spizzca.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        // create Producer Properties
        Properties properties = new Properties();
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-227b8376-fernando-spizzca.c.aivencloud.com:27517");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32*1024);

        // Connect Aiven Cluster
        properties.put("security.protocol", "SSL");
        properties.put("ssl.protocol", "TLS");
        // Opcional: senha específica da chave se for diferente da do keystore
        properties.put("ssl.key.password", "AVNS_QT5n5bcsLRmCv206GLP");

        // Keystore
        properties.put("ssl.keystore.location", "D:\\OneDrive\\Documentos\\Cursos\\Kafka\\certificados_aiven\\client.keystore.p12");
        properties.put("ssl.keystore.password", "AVNS_QT5n5bcsLRmCv206GLP");
        properties.put("ssl.keystore.type", "PKCS12");

        // Truststore
        properties.put("ssl.truststore.location", "D:\\OneDrive\\Documentos\\Cursos\\Kafka\\certificados_aiven\\client.truststore.jks");
        properties.put("ssl.truststore.password", "AVNS_QT5n5bcsLRmCv206GLP");
        properties.put("ssl.truststore.type", "JKS");

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        // Crie o cabeçalho User-Agent exigido pela Wikimedia
        Headers headers = new Headers.Builder()
                .add("User-Agent", "Spizzca (contato: fernando.spizzca@gmail.com)")
                .build();

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        // Adicione os headers ao builder
        builder.headers(headers);

        EventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // we produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(20);

//        // create a Producer Record
//        ProducerRecord<String, String> producerRecord =
//                new ProducerRecord<>("wikimedia.recentchange", "hello world");
//
//        // send data
//        producer.send(producerRecord);
//
//        // tell the producer to send all data and block until done -- synchronous
//        producer.flush();
//
//        // flush and close the producer
//        producer.close();
    }
}
