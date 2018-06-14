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

public final class EventCassandraClusterClient implements Initializer, Finalizer {

  private static Session session = null;
  private static final Logger LOG = LoggerFactory.getLogger(EventCassandraClusterClient.class);
  private static String eventCassSeeds = null;
  private static String eventCassDatacenter = null;
  private static String eventCassCluster = null;
  private static String eventCassKeyspace = null;

  public Session getCassandraSession() {
    return session;
  }

  public String getCassKeyspace() {
    return eventCassKeyspace;
  }

  public void initializeComponent(JsonObject config) {
    eventCassSeeds = config.getString("event.cassandra.seeds");
    eventCassDatacenter = config.getString("event.cassandra.datacenter");
    eventCassCluster = config.getString("event.cassandra.cluster");
    eventCassKeyspace = config.getString("event.cassandra.keyspace");
    LOG.info("eventCassSeeds : {} - eventCassKeyspace : {}", eventCassSeeds, eventCassKeyspace);

    Cluster cluster = Cluster.builder().withClusterName(eventCassCluster).addContactPoints(eventCassSeeds.split(Constants.COMMA)).withRetryPolicy(DefaultRetryPolicy.INSTANCE)
            .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
    session = cluster.connect(eventCassKeyspace);
    LOG.info("Event Cassandra Cluster Initialized successfully..");
  }

  public void finalizeComponent() {
    session.close();
  }

  private static class EventCassandraClusterClientHolder {
    public static final EventCassandraClusterClient INSTANCE = new EventCassandraClusterClient();
  }

  public static EventCassandraClusterClient instance() {
    return EventCassandraClusterClientHolder.INSTANCE;
  }
}
