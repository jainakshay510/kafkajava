package firstgroupapp.ak.helloproducer1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer1 {

    public static void main(String args[]) throws ExecutionException, InterruptedException {
        System.out.println("Hello World");


        final Logger log = LoggerFactory.getLogger(Producer1.class);

        String bootstrapServer="127.0.0.1:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);




        for(int i=0;i<10;i++) {

            String topic="my_first_topic";
            String data="Hi again in loop from java_"+Integer.toString(i);
            String key="id_"+Integer.toString(i);
            log.info("key"+key);
            ProducerRecord<String,String> producerRecord=new ProducerRecord<String, String>(topic,key,data);
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        log.info("Successfully received the details as: \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset" + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp());
                    } else {
                        log.error("Can't produce,getting error", e);
                    }

                }
            }).get();
        }
        producer.flush();
        producer.close();
    }
}
