package org.gooru.analytics.jobs.samples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.gooru.analytics.jobs.infra.EventCassandraClusterClient;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class ProcessCSVFile {
  private static final EventCassandraClusterClient eventCassandraClusterClient = EventCassandraClusterClient.instance();

  public static void main(String args[]) {
    // String questionsCsv = "/tmp/questions.csv";
    String resourceCsv = "/tmp/resource.csv";
    String collectionCsv = "/tmp/collections.csv";
    String assessments = "/tmp/assessments.csv";
    processCsv(collectionCsv, "collection", "migrate");
    processCsv(resourceCsv, "resource", "migrate");
    processCsv(assessments, "assessment", "migrate");
    /*
     * String standardsCsv = "/tmp/standards.csv"; processCsv(standardsCsv,
     * "standards", null);
     */
    System.out.println("Done");
  }

  private static void processCsv(String file, String type, String migrationType) {
    BufferedReader fileReader = null;
    try {
      String line = "";
      String cvsSplitBy = ",";

      fileReader = new BufferedReader(new FileReader(file));
      while ((line = fileReader.readLine()) != null) {

        // use comma as separator
        String[] country = line.split(cvsSplitBy);

        System.out.println("key:" + country[0].replace("\"", "").trim());
        if (migrationType != null && migrationType.equalsIgnoreCase("migrate")) {
          insertIntoQueue(country[0].replace("\"", "").trim(), type);
        } else {
          insertIntoTaxonomyParentNode(country[0].replace("\"", "").trim());
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (fileReader != null) {
        try {
          fileReader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void insertIntoQueue(String gooruOid, String type) {
    try {
      if (StringUtils.isNotBlank(gooruOid)) {
        Insert insertStmt = QueryBuilder.insertInto("event_logger_insights", "stat_publisher_queue").value("metrics_name", "migrateMetricsTemp")
                .value("gooru_oid", gooruOid).value("event_time", System.currentTimeMillis()).value("type", type);
        ResultSetFuture resultSetFuture = eventCassandraClusterClient.getCassandraSession().executeAsync(insertStmt);
        resultSetFuture.get();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void insertIntoTaxonomyParentNode(String key) {
    try {
      Insert insertStmt = QueryBuilder.insertInto("insights_qa", "taxonomy_parent_node").value("row_key", key).value("subject_id", "NA")
              .value("course_id", "NA").value("domain_id", "NA").value("standards_id", key).value("learning_targets_id", "NA").value("item_count", 1);
      ResultSetFuture resultSetFuture = eventCassandraClusterClient.getCassandraSession().executeAsync(insertStmt);
      resultSetFuture.get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
