package org.gooru.analytics.jobs.infra;

import org.gooru.analytics.jobs.infra.shutdown.Finalizer;
import org.gooru.analytics.jobs.infra.startup.Initializer;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

public final class PostgreSQLConnection implements Initializer, Finalizer {

  private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLConnection.class);
  private static String plSqlUrl = null;
  private static String plSqlUserName = null;
  private static String plSqlPassword = null;

  private static class PostgreSQLConnectionHolder {
    public static final PostgreSQLConnection INSTANCE = new PostgreSQLConnection();
  }

  public static PostgreSQLConnection instance() {
    return PostgreSQLConnectionHolder.INSTANCE;
  }

  public void initializeComponent(JsonObject config) {
    plSqlUrl = config.getString("postgresql.driverurl");
    plSqlUserName = config.getString("postgresql.username");
    plSqlPassword = config.getString("postgresql.password");

    try {
      if (!Base.hasConnection()) {
        Base.open("org.postgresql.Driver", plSqlUrl, plSqlUserName, plSqlPassword);
      }
      Base.openTransaction();
    } catch (Exception e) {
      LOG.error("Error while initializing postgreSQL....{}", e);
    }
  }

  public void finalizeComponent() {
    Base.close();
  }
}
