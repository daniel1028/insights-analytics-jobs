package org.gooru.analytics.jobs.constants;

public final class Constants {
  public static final String COMMA = ",";
  public static final String NA = "NA";
	public static final String FIELDS = "fields";
	public static final String EVENT_TIMIELINE = "event_timeline";
	public static final String EVENT_DETAIL = "event_detail";
	public static final String STAT_PUBLISHER_QUEUE = "stat_publisher_queue";
	public static final String METRICS = "metrics";
	public static final String VIEWS = "views";
	public static final String MIGRATE_METRICS = "migrateMetrics";
	public static final String COUNT_VIEWS = "count~views";
	public static final String TIME_SPENT_TOTAL = "time_spent~total";
	public static final String COUNT_COPY = "count~copy";
	public static final String TOTAL_TIMESPENT_IN_MS = "totalTimeSpentInMs";
	public static final String COPY = "copy";
	public static final String COUNT_RESOURCE_ADDED_PUBLIC = "count~resourceAddedPublic";
	public static final String ID = "id";
	public static final String VIEWS_COUNT = "viewsCount";
	public static final String COLLECTION_REMIX_COUNT = "collectionRemixCount";
	public static final String USED_IN_COLLECTION_COUNT = "usedInCollectionCount";
	public static final String COLLABORATOR_COUNT = "collaboratorCount";
	public static final String indexName = "gooru_local_statistics_v2";
	public static final String typeName = "statistics";
	public static final String STATISTICAL_DATA = "statistical_data";
	public static final String _CLUSTERING_KEY = "clustering_key";
	public static final String _METRICS_NAME = "metrics_name";
	public static final String _METRICS_VALUE = "metrics_value";
	public static final String TYPE = "type";
	public static final String DATA = "data";
	public static final String EVENT_NAME = "eventName";
	public static final String VIEWS_UPDATE = "views.update";
	public static final String PUBLISH_METRICS = "publishMetrics";
	public static final String CREATOR_ID = "creator_id";
	public static final String CREATOR_UID = "creator_uid";
	public static final String _GOORU_OID = "gooru_oid";
	public static final String COLLABORATOR = "collaborator";
	public static final String COLLABORATORS = "collaborators";
	public static final String UPDATED_AT = "updated_at";
	public static final String SYNC_JOBS_PROPERTIES = "sync_jobs_properties";
	public static final String _JOB_NAME = "job_name";
	public static final String PROPERTY_NAME = "property_name";
	public static final String PROPERTY_VALUE = "property_value";
	public static final String LAST_UPDATED_TIME = "last_updated_time";
	public static final String CONTENT_AUTHORIZED_USERS = "content_authorized_users";
	public static final String CLASS_MEMBERS = "class_members";
	public static final String GET_CLASS_COURSE = "select distinct class.id,col.course_id,class.updated_at from class class inner join collection col on col.course_id = class.course_id where col.is_deleted = false and col.updated_at > to_timestamp(?,'YYYY-MM-DD HH24:MI:SS') - interval '3 minutes';";
	public static final String GET_COURSE_COUNT = "select course_id as contentId, format, count(format) as totalCounts from collection where is_deleted = false and course_id = ? group by format,course_id;";
	public static final String GET_UNIT_COUNT = "select unit_id as contentId, format, count(format) as totalCounts from collection where is_deleted = false and course_id = ? group by format,unit_id;";
	public static final String GET_LESSON_COUNT = "select lesson_id as contentId, format, count(format) as totalCounts from collection where is_deleted = false and course_id = ? group by format,lesson_id;";
	public static final String COURSE_ID = "course_id";
	public static final String CLASS_UID = "class_uid";
	public static final String CLASS_ID = "class_id";
	public static final String MEMBERS = "members";
	public static final String CONTENT_UID = "content_uid";
	public static final String CONTENT_ID = "contentId";
	public static final String CONTENT_TYPE = "content_type";
	public static final String FORMAT = "format";
	public static final String TOTAL_COUNT = "total_count";
	public static final String TOTAL_COUNTS = "totalCounts";
	public static final String CLASS_CONTENT_COUNT = "class_content_count";
	public static final String UTC = "UTC";

    private Constants() {
        throw new AssertionError();
    }
}
