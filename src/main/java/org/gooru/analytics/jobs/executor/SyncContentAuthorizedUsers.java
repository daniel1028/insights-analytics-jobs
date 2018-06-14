package org.gooru.analytics.jobs.executor;

import java.text.SimpleDateFormat;
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
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import io.vertx.core.json.JsonObject;

public class SyncContentAuthorizedUsers implements JobInitializer {
  private static final Timer timer = new Timer();
  private static final String JOB_NAME = "sync_content_authorized_users";
  private static final AnalyticsUsageCassandraClusterClient analyticsUsageCassandraClusterClient = AnalyticsUsageCassandraClusterClient.instance();
  private static final PostgreSQLConnection postgreSQLConnection = PostgreSQLConnection.instance();
  private static final Logger LOG = LoggerFactory.getLogger(SyncContentAuthorizedUsers.class);
  private static final String GET_AUTHORIZED_USERS_QUERY =
          "select id,creator_id,collaborator,updated_at from class where class.updated_at > to_timestamp(?,'YYYY-MM-DD HH24:MI:SS') - interval '3 minutes';";
  private static String currentTime = null;
  private static final SimpleDateFormat minuteDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static long JOB_INTERVAL = 6000L;

  private static class SyncContentAuthorizedUsersHolder {
    public static final SyncContentAuthorizedUsers INSTANCE = new SyncContentAuthorizedUsers();
  }

  public static SyncContentAuthorizedUsers instance() {
    return SyncContentAuthorizedUsersHolder.INSTANCE;
  }

  public void deployJob(JsonObject config) {
    LOG.info("deploying SyncContentAuthorizedUsers....");
    JOB_INTERVAL = config.getLong("content.authorizers.sync.delay");
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
        List<Map> classList = Base.findAll(GET_AUTHORIZED_USERS_QUERY, currentTime);
        for (Map classCourseDetail : classList) {
          String classId = classCourseDetail.get(Constants.ID).toString();
          String creator = classCourseDetail.get(Constants.CREATOR_ID).toString();
          Object collabs = classCourseDetail.get(Constants.COLLABORATOR);
          updatedTime = classCourseDetail.get(Constants.UPDATED_AT).toString();
          HashSet<String> collaborators = null;
          LOG.info("class : " + classId);
          if (collabs != null) {
            JSONArray collabsArray = new JSONArray(collabs.toString());
            collaborators = new HashSet<>();
            for (int index = 0; index < collabsArray.length(); index++) {
              collaborators.add(collabsArray.getString(index));
            }
          }
          updateAuthorizedUsers(classId, creator, collaborators);
        }
        updateLastUpdatedTime(JOB_NAME, updatedTime == null ? currentTime : updatedTime);
        postgreSQLConnection.finalizeComponent();
      }
    };
    timer.scheduleAtFixedRate(task, 0, JOB_INTERVAL);

  }

  public SyncContentAuthorizedUsers() {
  }

  private static void updateAuthorizedUsers(String classId, String creator, HashSet<String> collabs) {
    try {
      Insert insert = QueryBuilder.insertInto(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(), Constants.CONTENT_AUTHORIZED_USERS)
              .value(Constants._GOORU_OID, classId).value(Constants.COLLABORATORS, collabs).value(Constants.CREATOR_UID, creator);

      ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession().executeAsync(insert);
      resultSetFuture.get();
    } catch (Exception e) {
      LOG.error("Error while updating authorized user details.", e);
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
