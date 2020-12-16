package kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.cdimascio.dotenv.Dotenv;
import jdk.internal.org.jline.utils.Log;

public class ProducerDemoWithCallback {
    Dotenv dotenv = Dotenv.load();
    private String bootstrapServer = dotenv.get("bootstrapserver");

    public ProducerDemoWithCallback() {

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
        for (int i = 0; i < 10; i++) {

            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Message Works! " + i);
            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes very time a record is succesffuly sent or an exceptiopn is thrown
                    Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
                    if (exception == null) {
                        logger.info("Received new metadata. \n" + "Topic:" + metadata.topic() + "\n" + "Partition:"
                                + metadata.partition() + "\n" + "Offset:" + metadata.offset() + "\n" + "Timestamp:"
                                + metadata.timestamp());
                    } else {
                        Log.error("Error while producing" + exception);
                    }

                }
            });
        }
        // flush data
        // producer.flush();

        // flush and close producer
        producer.close();
    }

}
