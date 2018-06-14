package org.gooru.analytics.jobs.executor;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.gooru.analytics.jobs.constants.Constants;
import org.gooru.analytics.jobs.infra.AnalyticsUsageCassandraClusterClient;
import org.gooru.analytics.jobs.infra.EventCassandraClusterClient;
import org.gooru.analytics.jobs.infra.KafkaClusterClient;
import org.gooru.analytics.jobs.infra.startup.JobInitializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import io.vertx.core.json.JsonObject;

public class StatMetricsPublisher implements JobInitializer {
  private static final Logger LOG = LoggerFactory.getLogger(StatDataMigration.class);
  private static String KAFKA_QUEUE_TOPIC = null;
  private static int QUEUE_LIMIT = 10;
  private static final Timer timer = new Timer();
  private static final long JOB_DELAY = 0;
  private static long JOB_INTERVAL = 6000L;
  private static final KafkaClusterClient producer = KafkaClusterClient.instance();
  private static final AnalyticsUsageCassandraClusterClient analyticsUsageCassandraClusterClient = AnalyticsUsageCassandraClusterClient.instance();
  private static final EventCassandraClusterClient eventCassandraClusterClient = EventCassandraClusterClient.instance();

  private static class StatMetricsPublisherHolder {
    public static final StatMetricsPublisher INSTANCE = new StatMetricsPublisher();
  }

  public static StatMetricsPublisher instance() {
    return StatMetricsPublisherHolder.INSTANCE;
  }

  public void deployJob(JsonObject config) {
    LOG.info("deploying StatMetricsPublisher....");

    QUEUE_LIMIT = config.getInteger("stat.publisher.queue.limit");
    JOB_INTERVAL = config.getInteger("stat.publisher.delay");
    KAFKA_QUEUE_TOPIC = config.getString("metrics.publisher.topic");

    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        ResultSet queueSet = getPublisherQueue(Constants.PUBLISH_METRICS);

        JSONObject statObject = new JSONObject();
        JSONArray jArray = new JSONArray();
        for (Row queue : queueSet) {
          ResultSet resultSet = getStatMetrics(queue.getString(Constants._GOORU_OID), Constants.VIEWS);
          for (Row result : resultSet) {
            JSONObject jObject = new JSONObject();
            jObject.put(Constants.ID, result.getString(Constants._CLUSTERING_KEY));
            jObject.put(Constants.VIEWS_COUNT, result.getLong(Constants._METRICS_VALUE));
            jObject.put(Constants.TYPE, queue.getString(Constants.TYPE));
            jArray.add(jObject);
          }
          deleteFromPublisherQueue(Constants.PUBLISH_METRICS, queue.getString(Constants._GOORU_OID));
        }
        statObject.put(Constants.EVENT_NAME, Constants.VIEWS_UPDATE);
        statObject.put(Constants.DATA, jArray);

        if (!jArray.isEmpty()) {
          LOG.info("message: " + statObject.toString());
          LOG.info("KAFKA_QUEUE_TOPIC: " + KAFKA_QUEUE_TOPIC);
          ProducerRecord<String, String> data = new ProducerRecord<String, String>(KAFKA_QUEUE_TOPIC, statObject.toString());
          try {
            LOG.info("going to publish");
            producer.getPublisher().send(data);
            LOG.info("Statistical data publishing completed by " + new Date());
          } catch (Exception e) {
            LOG.error("Exception while publish message", e);
          }
        }
      }
    };
    timer.scheduleAtFixedRate(task, JOB_DELAY, JOB_INTERVAL);

  }

  public StatMetricsPublisher() {
  }

  private static ResultSet getStatMetrics(String gooruOids, String metricsName) {
    ResultSet result = null;
    try {
      Statement select = QueryBuilder.select().all().from(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(), Constants.STATISTICAL_DATA)
              .where(QueryBuilder.eq(Constants._CLUSTERING_KEY, gooruOids)).and(QueryBuilder.eq(Constants._METRICS_NAME, metricsName))
              .setConsistencyLevel(ConsistencyLevel.QUORUM);
      ResultSetFuture resultSetFuture = (analyticsUsageCassandraClusterClient.getCassandraSession()).executeAsync(select);
      result = resultSetFuture.get();
    } catch (Exception e) {
      LOG.info("Error while get stat metrics..");
    }
    return result;
  }

  private static ResultSet getPublisherQueue(String metricsName) {
    ResultSet result = null;
    try {
      Statement select = QueryBuilder.select().all().from(eventCassandraClusterClient.getCassKeyspace(), Constants.STAT_PUBLISHER_QUEUE)
              .where(QueryBuilder.eq(Constants._METRICS_NAME, metricsName)).limit(QUEUE_LIMIT).setConsistencyLevel(ConsistencyLevel.QUORUM);
      ResultSetFuture resultSetFuture = (eventCassandraClusterClient.getCassandraSession()).executeAsync(select);
      result = resultSetFuture.get();
    } catch (Exception e) {
      LOG.info("Error while get stat publisher queue..");
    }
    return result;
  }

  private static void deleteFromPublisherQueue(String metricsName, String gooruOid) {
    try {
      LOG.info("Removing -" + gooruOid + "- from the statistical queue");
      Statement select = QueryBuilder.delete().all().from(eventCassandraClusterClient.getCassKeyspace(), Constants.STAT_PUBLISHER_QUEUE)
              .where(QueryBuilder.eq(Constants._METRICS_NAME, metricsName)).and(QueryBuilder.eq(Constants._GOORU_OID, gooruOid))
              .setConsistencyLevel(ConsistencyLevel.QUORUM);
      ResultSetFuture resultSetFuture = (eventCassandraClusterClient.getCassandraSession()).executeAsync(select);
      resultSetFuture.get();
    } catch (Exception e) {
      LOG.info("Error while delete stat publisher queue..");
    }
  }
}
