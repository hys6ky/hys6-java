package hyren.serv6.agent.job.biz.bean;

import fd.ng.core.annotation.DocClass;
import lombok.Data;

@DocClass(desc = "", author = "dhw", createdate = "2023-09-18")
@Data
public class StoreConnectionBean {

    private String agent_id;

    private String store_type;

    private String database_driver;

    private String jdbc_url;

    private String user_name;

    private String database_pwd;

    private String database_type;

    private String database_name;

    private String platform;

    private String hadoop_user_name;

    private String prncipal_name;

    private String keytab_user;

    private String sftp_host = "sftp_host";

    private String sftp_user = "sftp_user";

    private String sftp_pwd = "sftp_pwd";

    private String sftp_port = "sftp_port";

    private String external_root_path = "external_root_path";

    private String external_directory = "external_directory";

    private String solr_zk_url = "solr_zk_url";

    private String collection = "collection";

    private String zkhost = "zkhost";

    private String database_code = "database_code";

    private String increment_engine = "increment_engine";

    private String minPoolSize = "minPoolSize";

    private String maxPoolSize = "maxPoolSize";

    private String fetch_size = "fetch_size";

    private String kafka_zk_url_s = "kafka_zk_url";

    private String kafka_broker_s = "kafka_broker_s";
}
