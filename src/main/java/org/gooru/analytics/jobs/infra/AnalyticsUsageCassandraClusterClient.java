package org.gooru.analytics.jobs.infra;

import org.gooru.analytics.jobs.constants.Constants;
import org.gooru.analytics.jobs.infra.shutdown.Finalizer;
import org.gooru.analytics.jobs.infra.startup.Initializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

import io.vertx.core.json.JsonObject;

public final class AnalyticsUsageCassandraClusterClient implements Initializer, Finalizer {

  private static Session session = null;
  private static final Logger LOG = LoggerFactory.getLogger(AnalyticsUsageCassandraClusterClient.class);
  private static String analyticsCassSeeds = null;
  private static String analyticsCassDatacenter = null;
  private static String analyticsCassCluster = null;
  private static String analyticsCassKeyspace = null;

  public Session getCassandraSession() {
    return session;
  }

  public String getAnalyticsCassKeyspace() {
    return analyticsCassKeyspace;
  }

  private static class AnalyticsUsageCassandraClusterClientHolder {
    public static final AnalyticsUsageCassandraClusterClient INSTANCE = new AnalyticsUsageCassandraClusterClient();
  }

  public static AnalyticsUsageCassandraClusterClient instance() {
    return AnalyticsUsageCassandraClusterClientHolder.INSTANCE;
  }

  public void initializeComponent(JsonObject config) {
    analyticsCassSeeds = config.getString("analytics.cassandra.seeds");
    analyticsCassDatacenter = config.getString("analytics.cassandra.datacenter");
    analyticsCassCluster = config.getString("analytics.cassandra.cluster");
    analyticsCassKeyspace = config.getString("analytics.cassandra.keyspace");
    LOG.info("analyticsCassSeeds : {} - analyticsCassKeyspace : {} ", analyticsCassSeeds, analyticsCassKeyspace);

    Cluster cluster = Cluster.builder().withClusterName(analyticsCassCluster).addContactPoints(analyticsCassSeeds.split(Constants.COMMA))
            .withRetryPolicy(DefaultRetryPolicy.INSTANCE).withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
    session = cluster.connect(analyticsCassKeyspace);

    LOG.info("Analytics Cassandra initialized successfully...");
  }

  public void finalizeComponent() {
    session.close();
  }
}
