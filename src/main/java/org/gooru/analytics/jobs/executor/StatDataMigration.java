package org.gooru.analytics.jobs.executor;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.gooru.analytics.jobs.constants.Constants;
import org.gooru.analytics.jobs.infra.AnalyticsUsageCassandraClusterClient;
import org.gooru.analytics.jobs.infra.ArchivedCassandraClusterClient;
import org.gooru.analytics.jobs.infra.ElasticsearchClusterClient;
import org.gooru.analytics.jobs.infra.EventCassandraClusterClient;
import org.gooru.analytics.jobs.infra.startup.JobInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.retry.ConstantBackoff;

import io.vertx.core.json.JsonObject;

public class StatDataMigration implements JobInitializer {
  private static final Logger LOG = LoggerFactory.getLogger(StatDataMigration.class);
  private static int QUEUE_LIMIT = 10;
  private static String indexName = null;
  private static String typeName = null;
  private static final Timer timer = new Timer();
  private static final long JOB_DELAY = 0;
  private static long JOB_INTERVAL = 6000L;
  private static final AnalyticsUsageCassandraClusterClient analyticsUsageCassandraClusterClient = AnalyticsUsageCassandraClusterClient.instance();
  private static final EventCassandraClusterClient eventCassandraClusterClient = EventCassandraClusterClient.instance();
  private static final ArchivedCassandraClusterClient archivedCassandraClusterClient = ArchivedCassandraClusterClient.instance();
  private static final ElasticsearchClusterClient elasticsearchClusterClient = ElasticsearchClusterClient.instance();
  private static XContentBuilder contentBuilder = null;
  private static final PreparedStatement UPDATE_STATISTICAL_COUNTER_DATA = analyticsUsageCassandraClusterClient.getCassandraSession()
          .prepare("UPDATE statistical_data SET metrics_value = metrics_value+? WHERE clustering_key = ? AND metrics_name = ?");
  private static final PreparedStatement SELECT_STATISTICAL_COUNTER_DATA = analyticsUsageCassandraClusterClient.getCassandraSession()
          .prepare("SELECT metrics_value AS metrics FROM statistical_data WHERE clustering_key = ? AND metrics_name = ?");

  private static class StatDataMigrationHolder {
    public static final StatDataMigration INSTANCE = new StatDataMigration();
  }

  public static StatDataMigration instance() {
    return StatDataMigrationHolder.INSTANCE;
  }

  public void deployJob(JsonObject config) {
    LOG.info("deploying StatDataMigration....");
    LOG.info("Please make sure that we have loaded list of content oids in stat_publisher_queue column family.");

    indexName = config.getString("search.elasticsearch.index");
    typeName = config.getString("search.elasticsearch.type");
    QUEUE_LIMIT = config.getInteger("stat.migration.queue.limit");
    JOB_INTERVAL = config.getLong("stat.migration.delay");

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e1) {
      LOG.error("Thread InterruptedException", e1);
    }

    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        try {
          ResultSet queueSet = getPublisherQueue(Constants.MIGRATE_METRICS);
          for (Row queue : queueSet) {
            String gooruOid = queue.getString(Constants._GOORU_OID);
            ColumnList<String> statMetricsColumns = null;
            if (StringUtils.isNotBlank(gooruOid)) {
              statMetricsColumns = getStatMetrics(gooruOid);
            }
            if (statMetricsColumns != null) {
              contentBuilder = jsonBuilder().startObject();
              LOG.info("migrating id : " + gooruOid);
              long viewCount = 0L;
              long remixCount = 0L;
              long usedInCollectionCount = 0L;
              for (Column<String> statMetrics : statMetricsColumns) {

                switch (statMetrics.getName()) {
                case Constants.COUNT_VIEWS:
                  viewCount = statMetrics.getLongValue();
                  // updateStatisticalCounterData(gooruOid,VIEWS,
                  // viewCount);
                  balanceCounterData(gooruOid, Constants.VIEWS, viewCount);
                  break;
                case Constants.TIME_SPENT_TOTAL:
                  // updateStatisticalCounterData(gooruOid,TOTAL_TIMESPENT_IN_MS,statMetrics.getLongValue());
                  balanceCounterData(gooruOid, Constants.TOTAL_TIMESPENT_IN_MS, statMetrics.getLongValue());
                  break;
                case Constants.COUNT_COPY:
                  remixCount = statMetrics.getLongValue();
                  // updateStatisticalCounterData(gooruOid,COPY,
                  // remixCount);
                  balanceCounterData(gooruOid, Constants.COPY, remixCount);
                  break;
                case Constants.COUNT_RESOURCE_ADDED_PUBLIC:
                  usedInCollectionCount = statMetrics.getLongValue();
                  // updateStatisticalCounterData(gooruOid,COPY,
                  // remixCount);
                  balanceCounterData(gooruOid, Constants.USED_IN_COLLECTION_COUNT, usedInCollectionCount);
                  break;
                default:
                  LOG.info("Unused metric: " + statMetrics.getName());
                }

              }
              /**
               * Generate content builder to write in search index.
               */
              if (StringUtils.isNotBlank(gooruOid)) {
                XContentBuilder statistics = contentBuilder.startObject("statistics");
                statistics.field(Constants.ID, gooruOid);
                statistics.field(Constants.VIEWS_COUNT, viewCount);
                statistics.field(Constants.COLLECTION_REMIX_COUNT, remixCount);
                statistics.field(Constants.USED_IN_COLLECTION_COUNT, usedInCollectionCount);
                statistics.field(Constants.COLLABORATOR_COUNT, 0);
                statistics.endObject();
                indexingES(indexName, typeName, gooruOid, statistics);
              }
            }
            deleteFromPublisherQueue(Constants.MIGRATE_METRICS, gooruOid);
          }
        } catch (IOException e) {
          LOG.error("Error while migrating data.", e);
        }
        LOG.info("Job running at {}", new Date());
      }
    };
    timer.scheduleAtFixedRate(task, JOB_DELAY, JOB_INTERVAL);

  }

  private static ResultSet getPublisherQueue(String metricsName) {
    ResultSet result = null;
    try {
      Statement select = QueryBuilder.select().all().from(eventCassandraClusterClient.getCassKeyspace(), Constants.STAT_PUBLISHER_QUEUE)
              .where(QueryBuilder.eq(Constants._METRICS_NAME, metricsName)).limit(QUEUE_LIMIT).setConsistencyLevel(ConsistencyLevel.QUORUM);
      ResultSetFuture resultSetFuture = (eventCassandraClusterClient.getCassandraSession()).executeAsync(select);
      result = resultSetFuture.get();
    } catch (Exception e) {
      LOG.error("Error while reading publisher data into queue..", e);
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
      LOG.error("Error while reading publisher data into queue..", e);
    }
  }

  private static ColumnList<String> getStatMetrics(String gooruOid) {
    ColumnList<String> result = null;
    try {
      result = archivedCassandraClusterClient.getCassandraKeyspace().prepareQuery(archivedCassandraClusterClient.accessColumnFamily("live_dashboard"))
              .setConsistencyLevel(com.netflix.astyanax.model.ConsistencyLevel.CL_QUORUM).withRetryPolicy(new ConstantBackoff(2000, 5))
              .getKey("all~" + gooruOid).execute().getResult();

    } catch (Exception e) {
      LOG.error("Error while retrieve stat metrics..", e);
    }
    return result;
  }

  private static boolean updateStatisticalCounterData(String clusteringKey, String metricsName, Object metricsValue) {
    try {
      BoundStatement boundStatement = new BoundStatement(UPDATE_STATISTICAL_COUNTER_DATA);
      boundStatement.bind(metricsValue, clusteringKey, metricsName);
      analyticsUsageCassandraClusterClient.getCassandraSession().executeAsync(boundStatement);
    } catch (Exception e) {
      LOG.error("Error while update stat metrics..", e);
      return false;
    }
    return true;
  }

  private static boolean balanceCounterData(String clusteringKey, String metricsName, Long metricsValue) {
    try {
      BoundStatement selectBoundStatement = new BoundStatement(SELECT_STATISTICAL_COUNTER_DATA);
      selectBoundStatement.bind(clusteringKey, metricsName);
      ResultSetFuture resultFuture = analyticsUsageCassandraClusterClient.getCassandraSession().executeAsync(selectBoundStatement);
      ResultSet result = resultFuture.get();

      long existingValue = 0;
      if (result != null) {
        for (Row resultRow : result) {
          existingValue = resultRow.getLong(Constants.METRICS);
        }
      }
      long balancedMatrics = metricsValue - existingValue;

      BoundStatement boundStatement = new BoundStatement(UPDATE_STATISTICAL_COUNTER_DATA);
      boundStatement.bind(balancedMatrics, clusteringKey, metricsName);
      analyticsUsageCassandraClusterClient.getCassandraSession().executeAsync(boundStatement);
    } catch (Exception e) {
      LOG.error("Error while balance stat metrics..", e);
      return false;
    }
    return true;
  }

  private static void indexingES(String indexName, String indexType, String id, XContentBuilder contentBuilder) {
    elasticsearchClusterClient.getElsClient().prepareIndex(indexName, indexType, id).setSource(contentBuilder).execute().actionGet();
  }
}
