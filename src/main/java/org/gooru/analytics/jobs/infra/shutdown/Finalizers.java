package org.gooru.analytics.jobs.infra.shutdown;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gooru.analytics.jobs.infra.AnalyticsUsageCassandraClusterClient;
import org.gooru.analytics.jobs.infra.ArchivedCassandraClusterClient;
import org.gooru.analytics.jobs.infra.ElasticsearchClusterClient;
import org.gooru.analytics.jobs.infra.EventCassandraClusterClient;
import org.gooru.analytics.jobs.infra.KafkaClusterClient;
import org.gooru.analytics.jobs.infra.PostgreSQLConnection;

public class Finalizers implements Iterable<Finalizer> {

  private final Iterator<Finalizer> internalIterator;

  public Finalizers() {
    List<Finalizer> finalizers = new ArrayList<>();
    finalizers.add(AnalyticsUsageCassandraClusterClient.instance());
    finalizers.add(EventCassandraClusterClient.instance());
    finalizers.add(ArchivedCassandraClusterClient.instance());
    finalizers.add(KafkaClusterClient.instance());
    finalizers.add(ElasticsearchClusterClient.instance());
    finalizers.add(PostgreSQLConnection.instance());
    internalIterator = finalizers.iterator();
  }

  @Override
  public Iterator<Finalizer> iterator() {
    return new Iterator<Finalizer>() {

      @Override
      public boolean hasNext() {
        return internalIterator.hasNext();
      }

      @Override
      public Finalizer next() {
        return internalIterator.next();
      }

    };
  }

}
