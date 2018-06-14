package org.gooru.analytics.jobs.executor;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

import org.gooru.analytics.jobs.constants.Constants;
import org.gooru.analytics.jobs.infra.AnalyticsUsageCassandraClusterClient;
import org.gooru.analytics.jobs.infra.PostgreSQLConnection;
import org.gooru.analytics.jobs.infra.startup.JobInitializer;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import io.vertx.core.json.JsonObject;

public class SyncClassMembers implements JobInitializer {
  private static final Timer timer = new Timer();
  private static final String JOB_NAME = "sync_class_members";
  private static final AnalyticsUsageCassandraClusterClient analyticsUsageCassandraClusterClient = AnalyticsUsageCassandraClusterClient.instance();
  private static final PostgreSQLConnection postgreSQLConnection = PostgreSQLConnection.instance();
  private static final Logger LOG = LoggerFactory.getLogger(SyncClassMembers.class);
  private static final String GET_MEMBERS_QUERY =
          "select class_id,array_agg(user_id) as members ,now() as updated_at from class_member where class_member_status = 'joined' and updated_at > to_timestamp(?,'YYYY-MM-DD HH24:MI:SS') - interval '3 minutes' group by class_id;";
  private static String currentTime = null;
  private static final SimpleDateFormat minuteDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static long JOB_INTERVAL = 6000L;

  private static class SyncClassMembersHolder {
    public static final SyncClassMembers INSTANCE = new SyncClassMembers();
  }

  public static SyncClassMembers instance() {
    return SyncClassMembersHolder.INSTANCE;
  }

  public void deployJob(JsonObject config) {
    LOG.info("deploying SyncClassMembers....");
    JOB_INTERVAL = config.getLong("class.members.sync.delay");
    minuteDateFormatter.setTimeZone(TimeZone.getTimeZone(Constants.UTC));
    final String jobLastUpdatedTime = getLastUpdatedTime();
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        postgreSQLConnection.initializeComponent(config);
        if (currentTime != null) {
          currentTime = minuteDateFormatter.format(new Date());
        } else {
          currentTime = jobLastUpdatedTime;
        }
        LOG.info("currentTime:" + currentTime);
        String updatedTime = null;
        List<Map> classList = Base.findAll(GET_MEMBERS_QUERY, currentTime);
        for (Map membersList : classList) {
          String classId = membersList.get(Constants.CLASS_ID).toString();
          String classMembers = membersList.get(Constants.MEMBERS).toString();
          updatedTime = membersList.get(Constants.UPDATED_AT).toString();
          LOG.info("classId : " + classId);
          HashSet<String> members = null;
          if (classMembers != null) {
            members = new HashSet<>();
            members.addAll(Arrays.asList((classMembers.replace("}", "").replace("{", "")).split(",")));
          }
          updateClassMembers(classId, members);
        }
        updateLastUpdatedTime(JOB_NAME, updatedTime == null ? currentTime : updatedTime);
        postgreSQLConnection.finalizeComponent();
      }
    };
    timer.scheduleAtFixedRate(task, 0, JOB_INTERVAL);

  }

  private static void updateClassMembers(String classId, HashSet<String> members) {
    try {
      Insert insert = QueryBuilder.insertInto(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(), Constants.CLASS_MEMBERS)
              .value(Constants.CLASS_ID, classId).value(Constants.MEMBERS, members);

      ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession().executeAsync(insert);
      resultSetFuture.get();
    } catch (Exception e) {
      LOG.error("Error while updating class members details.", e);
    }
  }

  private static String getLastUpdatedTime() {
    try {
      Statement select = QueryBuilder.select().all()
              .from(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(), Constants.SYNC_JOBS_PROPERTIES)
              .where(QueryBuilder.eq(Constants._JOB_NAME, JOB_NAME)).and(QueryBuilder.eq(Constants.PROPERTY_NAME, Constants.LAST_UPDATED_TIME));
      ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession().executeAsync(select);
      ResultSet result = resultSetFuture.get();
      for (Row r : result) {
        return r.getString(Constants.PROPERTY_VALUE);
      }
    } catch (Exception e) {
      LOG.error("Error while reading job last updated time.{}", e);
    }
    return minuteDateFormatter.format(new Date());
  }

  private static void updateLastUpdatedTime(String jobName, String updatedTime) {
    try {
      Insert insertStatmt = QueryBuilder.insertInto(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(), Constants.SYNC_JOBS_PROPERTIES)
              .value(Constants._JOB_NAME, jobName).value(Constants.PROPERTY_NAME, Constants.LAST_UPDATED_TIME)
              .value(Constants.PROPERTY_VALUE, updatedTime);

      ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession().executeAsync(insertStatmt);
      resultSetFuture.get();
    } catch (Exception e) {
      LOG.error("Error while updating last updated time.", e);
    }
  }
}
