package org.gooru.analytics.jobs.samples;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SampleProducer {
  public static void main(String[] args) throws IOException {
    // set up the producer
    KafkaProducer<String, String> producer;
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 0);
    props.put("block.on.buffer.full", true);
    props.put("auto.commit.interval.ms", 1000);

    producer = new KafkaProducer<>(props);

    try {
      for (int i = 0; i < 1000; i++) {
        // send lots of messages
        System.out.println("pushing events....");
        producer.send(new ProducerRecord<String, String>("QA-CONTENT-LOG-TEST", "Testing by daniel"));

      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      producer.close();
    }

  }

}
