package org.gooru.analytics.jobs.infra;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.gooru.analytics.jobs.infra.shutdown.Finalizer;
import org.gooru.analytics.jobs.infra.startup.Initializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

public class KafkaClusterClient implements Initializer, Finalizer {

  private static KafkaProducer<String, String> producer = null;
  private static String kafkaBrokers = null;
  private static final Logger LOG = LoggerFactory.getLogger(KafkaClusterClient.class);

  private static class KafkaConnectionHolder {
    public static final KafkaClusterClient INSTANCE = new KafkaClusterClient();
  }

  public static KafkaClusterClient instance() {
    return KafkaConnectionHolder.INSTANCE;
  }

  public void initializeComponent(JsonObject config) {
    kafkaBrokers = config.getString("kafka.brokers");
    LOG.info("kafkaBrokers : {} ", kafkaBrokers);

    Properties props = new Properties();
    props.put("bootstrap.servers", kafkaBrokers);
    props.put("acks", "all");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 0);
    props.put("block.on.buffer.full", true);
    props.put("auto.commit.interval.ms", 1000);
    producer = new KafkaProducer<>(props);
    LOG.info("Kafka initialized successfully...");
  }

  public KafkaProducer<String, String> getPublisher() {
    return producer;
  }

  public void finalizeComponent() {
    producer.close();
  }
}
