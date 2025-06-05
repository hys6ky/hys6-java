package hyren.serv6.m.util.dbConf.storagelayer;

import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.entity.DatabaseInfo;
import hyren.serv6.m.util.dbConf.DataBaseType;
import java.util.*;

public class StorageTypeKeyV2 {

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
}
