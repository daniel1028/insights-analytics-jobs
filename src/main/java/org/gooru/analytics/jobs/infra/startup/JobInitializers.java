package org.gooru.analytics.jobs.infra.startup;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gooru.analytics.jobs.executor.StatDataMigration;
import org.gooru.analytics.jobs.executor.StatMetricsPublisher;
import org.gooru.analytics.jobs.executor.SyncClassMembers;
import org.gooru.analytics.jobs.executor.SyncContentAuthorizedUsers;
import org.gooru.analytics.jobs.executor.SyncTotalContentCounts;

public class JobInitializers implements Iterable<JobInitializer> {

  private final Iterator<JobInitializer> internalIterator;

  public JobInitializers() {
    List<JobInitializer> initializers = new ArrayList<>();
    initializers.add(SyncTotalContentCounts.instance());
    initializers.add(SyncContentAuthorizedUsers.instance());
    initializers.add(SyncClassMembers.instance());
    initializers.add(StatMetricsPublisher.instance());
    initializers.add(StatDataMigration.instance());
    internalIterator = initializers.iterator();
  }

  @Override
  public Iterator<JobInitializer> iterator() {
    return new Iterator<JobInitializer>() {

      @Override
      public boolean hasNext() {
        return internalIterator.hasNext();
      }

      @Override
      public JobInitializer next() {
        return internalIterator.next();
      }

    };
  }

}
