package org.gooru.analytics.jobs.executor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.gooru.analytics.jobs.constants.Constants;
import org.gooru.analytics.jobs.infra.ArchivedCassandraClusterClient;
import org.gooru.analytics.jobs.infra.EventCassandraClusterClient;
import org.gooru.analytics.jobs.infra.startup.JobInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.ConstantBackoff;

import io.vertx.core.json.JsonObject;

public class EventMigration implements JobInitializer {
  private static final Logger LOG = LoggerFactory.getLogger(EventMigration.class);
  private static final SimpleDateFormat minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
  private static final EventCassandraClusterClient eventCassandraClusterClient = EventCassandraClusterClient.instance();
  private static final ArchivedCassandraClusterClient archivedCassandraClusterClient = ArchivedCassandraClusterClient.instance();
  private static final PreparedStatement insertEvents =
          (eventCassandraClusterClient.getCassandraSession()).prepare("INSERT INTO events(event_id,fields)VALUES(?,?)");
  private static final PreparedStatement insertEventTimeLine =
          (eventCassandraClusterClient.getCassandraSession()).prepare("INSERT INTO events_timeline(event_time,event_id)VALUES(?,?);");

  private static class EventMigrationHolder {
    public static final EventMigration INSTANCE = new EventMigration();
  }

  public static EventMigration instance() {
    return EventMigrationHolder.INSTANCE;
  }

  public void deployJob(JsonObject config) {

    LOG.info("deploying EventMigration....");
    try {
      String start = "201508251405";
      String end = "201508251405";

      Long startTime = minuteDateFormatter.parse(start).getTime();
      LOG.info("startTime : " + start);
      Long endTime = minuteDateFormatter.parse(end).getTime();
      LOG.info("endTime : " + end);
      // String start = "201508251405";
      // Long endTime = new Date().getTime();

      for (Long startDate = startTime; startDate < endTime;) {
        String currentDate = minuteDateFormatter.format(new Date(startDate));
        LOG.info("Running for :" + currentDate);
        // Incrementing time - one minute
        ColumnList<String> et = readWithKey(Constants.EVENT_TIMIELINE, currentDate);
        for (String eventId : et.getColumnNames()) {
          ColumnList<String> ef = readWithKey(Constants.EVENT_DETAIL, et.getStringValue(eventId, Constants.NA));
          // Insert event_time_line
          insertData(currentDate, et.getStringValue(eventId, Constants.NA), insertEventTimeLine);
          // Insert events
          insertData(et.getStringValue(eventId, Constants.NA), ef.getStringValue(Constants.FIELDS, Constants.NA), insertEvents);
        }
        startDate = new Date(startDate).getTime() + 60000;
        Thread.sleep(200);
      }
    } catch (ParseException | InterruptedException e) {
      if (e instanceof ArrayIndexOutOfBoundsException) {
        LOG.info("startTime or endTime can not be null. Please make sure the class execution format as below.");
        LOG.info("java -classpath build/libs/migration-scripts-fat.jar: org.gooru.migration.jobs.EventMigration 201508251405 201508251410");
      } else {
        LOG.error("Something went wrong...", e);
      }
      System.exit(500);
    }

  }

  public static ColumnList<String> readWithKey(String cfName, String key) {

    ColumnList<String> result = null;
    try {
      result = (archivedCassandraClusterClient.getCassandraKeyspace()).prepareQuery(archivedCassandraClusterClient.accessColumnFamily(cfName))
              .setConsistencyLevel(ConsistencyLevel.CL_QUORUM).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute().getResult();

    } catch (Exception e) {
      LOG.error("Failure in reading with key", e);
    }

    return result;
  }

  public static void insertData(String key, String column, PreparedStatement preparedStatement) {
    try {
      BoundStatement boundStatement = new BoundStatement(preparedStatement);
      boundStatement.bind(key, column);
      (eventCassandraClusterClient.getCassandraSession()).executeAsync(boundStatement);
    } catch (Exception e) {
      LOG.error("Inserting Data failed with exception: ", e);
    }
  }

}
