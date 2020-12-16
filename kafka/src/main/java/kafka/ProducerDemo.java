package kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import io.github.cdimascio.dotenv.Dotenv;

public class ProducerDemo {
    Dotenv dotenv = Dotenv.load();
    private String bootstrapServer = dotenv.get("bootstrapserver");

    public ProducerDemo() {

        Properties properties = new Properties();
        // hardcoded
        // properties.setProperty("bootstrap.servers", bootstrapServer);
        // properties.setProperty("key.serializer", StringSerializer.class.getName());
        // properties.setProperty("value.serializer", StringSerializer.class.getName());

        // better way and preventing typos
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Message Works!");
        // send data
        producer.send(record);

        // flush data
        // producer.flush();

        // flush and close producer
        producer.close();
    }

}
