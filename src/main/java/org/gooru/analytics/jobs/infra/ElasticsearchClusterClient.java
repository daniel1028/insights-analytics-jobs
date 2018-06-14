package org.gooru.analytics.jobs.infra;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.gooru.analytics.jobs.infra.shutdown.Finalizer;
import org.gooru.analytics.jobs.infra.startup.Initializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

public class ElasticsearchClusterClient implements Initializer, Finalizer {

  private static Client client = null;
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClusterClient.class);
  private static String searchElsCluster = null;
  private static String searchElsHost = null;
  private static String searchIndexName = null;
  private static String searchTypeName = null;

  private static class ElasticsearchClusterClientHolder {
    public static final ElasticsearchClusterClient INSTANCE = new ElasticsearchClusterClient();
  }

  public static ElasticsearchClusterClient instance() {
    return ElasticsearchClusterClientHolder.INSTANCE;
  }

  public void initializeComponent(JsonObject config) {
    searchElsCluster = config.getString("search.elasticsearch.cluster");
    searchElsHost = config.getString("search.elasticsearch.ip");
    searchIndexName = config.getString("search.elasticsearch.index");
    searchTypeName = config.getString("search.elasticsearch.type");
    LOG.info("searchElsHost : {} - searchElsCluster : {} ", searchElsHost, searchElsCluster);

    Settings settings = Settings.settingsBuilder().put("cluster.name", searchElsCluster).put("client.transport.sniff", true).build();
    TransportClient transportClient;
    try {
      transportClient = TransportClient.builder().settings(settings).build()
              .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(searchElsHost), 9300));
      client = transportClient;
    } catch (UnknownHostException e) {
      LOG.error("Error while initializing els.", e);
    }
    LOG.info("ELS initialized successfully...");
  }

  public Client getElsClient() {
    return client;
  }

  public void finalizeComponent() {
    client.close();
  }
}
