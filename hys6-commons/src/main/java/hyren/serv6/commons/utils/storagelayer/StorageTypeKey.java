package hyren.serv6.commons.utils.storagelayer;

import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.entity.DatabaseInfo;
import hyren.serv6.commons.utils.constant.DataBaseType;
import java.util.*;

public class StorageTypeKey {

    private static final Map<String, List<String>> FINALLY_STORAGE_KEYS = new HashMap<>();

    private static final List<String> UPDATE_FINALLY_STORAGE_KEYS = new ArrayList<>();

    public static final String database_driver = "database_driver";

    public static final String jdbc_url = "jdbc_url";

    public static final String user_name = "user_name";

    public static final String database_pwd = "database_pwd";

    public static final String database_type = "database_type";

    public static final String database_name = "database_name";

    public static final String core_site = "core-site.xml";

    public static final String hdfs_site = "hdfs-site.xml";

    public static final String yarn_site = "yarn-site.xml";

    public static final String hbase_site = "hbase-site.xml";

    public static final String mapred_site = "mapred-site.xml";

    public static final String hive_site = "hive-site.xml";

    public static final String platform = "platform";

    public static final String hadoop_user_name = "hadoop_user_name";

    public static final String prncipal_name = "prncipal_name";

    public static final String keytab_file = "keytab_file.keytab";

    public static final String keytab_user = "keytab_user";

    public static final String krb5_conf = "krb5.conf";

    public static final String sftp_host = "sftp_host";

    public static final String sftp_user = "sftp_user";

    public static final String sftp_pwd = "sftp_pwd";

    public static final String sftp_port = "sftp_port";

    public static final String external_root_path = "external_root_path";

    public static final String external_directory = "external_directory";

    public static final String solr_zk_url = "solr_zk_url";

    public static final String collection = "collection";

    public static final String zkhost = "zkhost";

    public static final String database_code = "database_code";

    public static final String increment_engine = "increment_engine";

    public static final String minPoolSize = "minPoolSize";

    public static final String maxPoolSize = "maxPoolSize";

    public static final String fetch_size = "fetch_size";

    public static final String kafka_zk_url_s = "kafka_zk_url";

    public static final String kafka_broker_s = "kafka_broker_s";

    static {
        List<String> databaseKeys = new ArrayList<>(Arrays.asList(database_driver, jdbc_url, user_name, database_pwd, database_type, database_name));
        FINALLY_STORAGE_KEYS.put(Store_type.DATABASE.getCode(), databaseKeys);
        List<String> hiveKeys = new ArrayList<>(Arrays.asList(database_driver, jdbc_url, user_name, database_pwd, database_name));
        FINALLY_STORAGE_KEYS.put(Store_type.HIVE.getValue(), hiveKeys);
        List<String> carbonKeys = new ArrayList<>(Arrays.asList(database_driver, jdbc_url, user_name, database_pwd, database_name));
        FINALLY_STORAGE_KEYS.put(Store_type.CARBONDATA.getValue(), carbonKeys);
        List<String> hiveExternalTableKeys = new ArrayList<>(Arrays.asList(database_driver, jdbc_url, user_name, database_pwd, database_name, database_code, platform, hadoop_user_name, prncipal_name, keytab_user, krb5_conf, keytab_file, core_site, hdfs_site, hive_site, hbase_site, mapred_site, yarn_site));
        FINALLY_STORAGE_KEYS.put(Store_type.HIVE.getValue() + "_" + IsFlag.Shi.getCode(), hiveExternalTableKeys);
        List<String> hbaseKeys = new ArrayList<>(Arrays.asList(zkhost, increment_engine, database_driver, jdbc_url, user_name, database_pwd, database_name, platform, hadoop_user_name, prncipal_name, keytab_user, krb5_conf, keytab_file, core_site, hdfs_site, hive_site, hbase_site, mapred_site, yarn_site));
        FINALLY_STORAGE_KEYS.put(Store_type.HBASE.getValue(), hbaseKeys);
        List<String> solrKeys = new ArrayList<>(Arrays.asList(solr_zk_url, collection));
        FINALLY_STORAGE_KEYS.put(Store_type.SOLR.getValue(), solrKeys);
        List<String> kafkaKeys = new ArrayList<>(Arrays.asList(kafka_zk_url_s, kafka_broker_s));
        FINALLY_STORAGE_KEYS.put(Store_type.KAFKA.getValue(), kafkaKeys);
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            List<String> dbNameList = SqlOperator.queryOneColumnList(db, "select database_name from " + DatabaseInfo.TableName);
            if (!dbNameList.isEmpty()) {
                dbNameList.forEach(dbName -> {
                    List<String> externalTableKeys = DataBaseType.getDatabase(dbName).getExternalTableKeys();
                    if (externalTableKeys != null) {
                        FINALLY_STORAGE_KEYS.put(dbName + "_" + IsFlag.Shi.getCode(), externalTableKeys);
                    }
                });
            }
        }
        UPDATE_FINALLY_STORAGE_KEYS.add(krb5_conf);
        UPDATE_FINALLY_STORAGE_KEYS.add(keytab_file);
        UPDATE_FINALLY_STORAGE_KEYS.add(core_site);
        UPDATE_FINALLY_STORAGE_KEYS.add(hdfs_site);
        UPDATE_FINALLY_STORAGE_KEYS.add(hive_site);
        UPDATE_FINALLY_STORAGE_KEYS.add(hbase_site);
        UPDATE_FINALLY_STORAGE_KEYS.add(mapred_site);
        UPDATE_FINALLY_STORAGE_KEYS.add(yarn_site);
    }

    public static Map<String, List<String>> getFinallyStorageKeys() {
        return FINALLY_STORAGE_KEYS;
    }

    public static List<String> getUpdateFinallyStorageKeys() {
        return UPDATE_FINALLY_STORAGE_KEYS;
    }
}
