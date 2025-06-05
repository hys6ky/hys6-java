package hyren.serv6.hadoop.commons.loginAuth.impl;

import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import java.io.File;

@Slf4j
public class C80LoginAuthImpl extends AbsLoginAuthImpl {

    public C80LoginAuthImpl() {
        this(System.getProperty("user.dir") + File.separator + "conf" + File.separator);
    }

    public C80LoginAuthImpl(String conf_dir) {
        confDir = conf_dir;
        PATH_TO_KEYTAB = conf_dir + StorageTypeKey.keytab_file;
        PATH_TO_KRB5_CONF = conf_dir + StorageTypeKey.krb5_conf;
        PATH_TO_JAAS = conf_dir + "jaas.conf";
        PATH_TO_CORE_SITE_XML = conf_dir + StorageTypeKey.core_site;
        PATH_TO_HDFS_SITE_XML = conf_dir + StorageTypeKey.hdfs_site;
        PATH_TO_HBASE_SITE_XML = conf_dir + StorageTypeKey.hbase_site;
        PATH_TO_MAPRED_SITE_XML = conf_dir + StorageTypeKey.mapred_site;
        PATH_TO_YARN_SITE_XML = conf_dir + StorageTypeKey.yarn_site;
        log.info("platform: FIC80, go to the " + conf_dir + " configuration file!");
    }

    @Override
    public synchronized Configuration login(String principle_name, String userKeytabPath, String krb5ConfPath, Configuration conf) {
        PRNCIPAL_NAME = principle_name;
        throw new BusinessException("FI C80 平台登录认证暂未实现!");
    }
}
