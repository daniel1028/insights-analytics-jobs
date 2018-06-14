package org.gooru.analytics.jobs.infra.startup;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gooru.analytics.jobs.infra.AnalyticsUsageCassandraClusterClient;
import org.gooru.analytics.jobs.infra.ArchivedCassandraClusterClient;
import org.gooru.analytics.jobs.infra.ElasticsearchClusterClient;
import org.gooru.analytics.jobs.infra.EventCassandraClusterClient;
import org.gooru.analytics.jobs.infra.KafkaClusterClient;
import org.gooru.analytics.jobs.infra.PostgreSQLConnection;

public class Initializers implements Iterable<Initializer> {

  private final Iterator<Initializer> internalIterator;

  public Initializers() {
    List<Initializer> initializers = new ArrayList<>();
    initializers.add(AnalyticsUsageCassandraClusterClient.instance());
    initializers.add(EventCassandraClusterClient.instance());
    initializers.add(ArchivedCassandraClusterClient.instance());
    initializers.add(KafkaClusterClient.instance());
    initializers.add(ElasticsearchClusterClient.instance());
    initializers.add(PostgreSQLConnection.instance());
    internalIterator = initializers.iterator();
  }

  @Override
  public Iterator<Initializer> iterator() {
    return new Iterator<Initializer>() {

      @Override
      public boolean hasNext() {
        return internalIterator.hasNext();
      }

      @Override
      public Initializer next() {
        return internalIterator.next();
      }

    };
  }

}
