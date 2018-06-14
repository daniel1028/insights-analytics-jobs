package org.gooru.analytics.jobs.infra;

import org.gooru.analytics.jobs.infra.shutdown.Finalizer;
import org.gooru.analytics.jobs.infra.startup.Initializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import io.vertx.core.json.JsonObject;

public final class ArchivedCassandraClusterClient implements Initializer, Finalizer {

  private static Keyspace cassandraKeyspace = null;
  private static AstyanaxContext<Keyspace> context = null;
  private static final Logger LOG = LoggerFactory.getLogger(ArchivedCassandraClusterClient.class);
  private static String archivedCassSeeds = null;
  private static String archivedCassDatacenter = null;
  private static String archivedCassCluster = null;
  private static String archivedCassKeyspace = null;

  public void initializeComponent(JsonObject config) {

    archivedCassSeeds = config.getString("archived.cassandra.seeds");
    archivedCassDatacenter = config.getString("archived.cassandra.datacenter");
    archivedCassCluster = config.getString("archived.cassandra.cluster");
    archivedCassKeyspace = config.getString("archived.cassandra.keyspace");

    LOG.info("archivedCassSeeds : {} - archivedCassKeyspace : {} ", archivedCassSeeds, archivedCassKeyspace);
    ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(9160).setSeeds(archivedCassSeeds)
            .setSocketTimeout(30000).setMaxTimeoutWhenExhausted(2000).setMaxConnsPerHost(10).setInitConnsPerHost(1);
    poolConfig.setLocalDatacenter(archivedCassDatacenter);
    context = new AstyanaxContext.Builder().forCluster(archivedCassCluster).forKeyspace(archivedCassKeyspace)
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setTargetCassandraVersion("2.1.4").setCqlVersion("3.0.0")
                    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE).setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
            .withConnectionPoolConfiguration(poolConfig).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());
    context.start();
    cassandraKeyspace = context.getClient();
    LOG.info("Archieved Cassandra initialized successfully");
  }

  public Keyspace getCassandraKeyspace() {
    return cassandraKeyspace;
  }

  public void finalizeComponent() {
    context.shutdown();
  }

  private static class ArchivedCassandraClusterClientHolder {
    public static final ArchivedCassandraClusterClient INSTANCE = new ArchivedCassandraClusterClient();
  }

  public static ArchivedCassandraClusterClient instance() {
    return ArchivedCassandraClusterClientHolder.INSTANCE;
  }

  public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

    ColumnFamily<String, String> aggregateColumnFamily;

    aggregateColumnFamily = new ColumnFamily<>(columnFamilyName, StringSerializer.get(), StringSerializer.get());

    return aggregateColumnFamily;
  }
}
