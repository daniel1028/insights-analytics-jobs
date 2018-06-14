package org.gooru.analytics.jobs.infra.startup;

import io.vertx.core.json.JsonObject;

public interface JobInitializer {

  void deployJob(JsonObject config);
}
