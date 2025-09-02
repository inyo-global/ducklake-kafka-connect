package com.inyo.ducklake.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class DucklakeSinkConfig extends AbstractConfig {

    /**
     * This class used iceberg-kafka-connect as reference implementation
     */

    public static final String VERSION = "1.0.0";
    public static final String DUCKLAKE_CATALOG_URI = "ducklake.catalog_uri";

    public static final ConfigDef CONFIG_DEF = newConfigDef();

    static final String TABLES_PROP = "ducklake.tables";
    static final String DATA_PATH = "ducklake.data_path";
    static final String TOPICS_TABLES_MAP = "topic2table.map";
    static final String S3_URL_STYLE = "s3.url_style";
    static final String S3_USE_SSL = "s3.use_ssl";
    static final String S3_ENDPOINT = "s3.endpoint";
    static final String S3_ACCESS_KEY_ID = "s3.access_key_id";
    static final String S3_SECRET_ACCESS_KEY = "s3._secret_access_key";


    private static ConfigDef newConfigDef() {
        return new ConfigDef()
                .define(DUCKLAKE_CATALOG_URI, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Ducklake catalog URI, e.g., postgres:dbname=ducklake_catalog host=localhost")
                .define(TABLES_PROP, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Comma-delimited list of destination tables")
                .define(TOPICS_TABLES_MAP, ConfigDef.Type.STRING, "", new TopicToTableValidator(),
                        ConfigDef.Importance.LOW,
                        "Map of topics to tables (optional). Format : comma-separated "
                                + "tuples, e.g. <topic-1>:<table-1>,<topic-2>:<table-2>,...")
                .define(DATA_PATH, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Data path in the format eg s3://my-bucket/path/, gs://my-bucket/path/, file:///path/")
                .define(S3_URL_STYLE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Either vhost or path")
                .define(S3_USE_SSL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Whether to use HTTPS or HTTP")
                .define(S3_ENDPOINT, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Specify a custom S3 endpoint")
                .define(S3_ACCESS_KEY_ID, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The ID of the key to use")
                .define(S3_SECRET_ACCESS_KEY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The secret of the key to use");
    }

    public DucklakeSinkConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    public String getDucklakeCatalogUri() {
        return getString(DUCKLAKE_CATALOG_URI);
    }

    public String getTables() {
        return getString(TABLES_PROP);
    }

    public String getS3UrlStyle() {
        return getString(S3_URL_STYLE);
    }

    public String getS3UseSsl() {
        return getString(S3_USE_SSL);
    }

    public String getS3Endpoint() {
        return getString(S3_ENDPOINT);
    }

    public String getS3AccessKeyId() {
        return getString(S3_ACCESS_KEY_ID);
    }

    public String getS3SecretAccessKey() {
        return getString(S3_SECRET_ACCESS_KEY);
    }

    public String getDataPath() {
        return getString(DATA_PATH);
    }

    public Map<String, String> getTopicToTableMap() {
        try {
            return TopicToTableValidator.parseTopicToTableMap(getString(TOPICS_TABLES_MAP));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
