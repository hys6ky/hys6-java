package hyren.serv6.hadoop.commons.hadoop_helper;

import fd.ng.core.utils.FileNameUtils;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.File;

@Slf4j
public class ConfigurationOperator {

    protected static String confDir;

    private static String configPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

    public String PRNCIPAL_NAME;

    public String PATH_TO_KEYTAB;

    public String PATH_TO_KRB5_CONF;

    public String PATH_TO_JAAS;

    public String PATH_TO_CORE_SITE_XML;

    public String PATH_TO_HDFS_SITE_XML;

    public String PATH_TO_HBASE_SITE_XML;

    public String PATH_TO_MAPRED_SITE_XML;

    public String PATH_TO_YARN_SITE_XML;

    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";

    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");

    public ConfigurationOperator() {
        this(configPath);
    }

    public ConfigurationOperator(String confDir) {
        this(confDir, "admin@HADOOP.COM");
    }

    public ConfigurationOperator(long dsl_id, DatabaseWrapper db) {
        this(ProcessingData.getLayerBean(dsl_id, db));
    }

    public ConfigurationOperator(LayerBean layerBean) {
        this(FileNameUtils.normalize(Constant.STORECONFIGPATH + layerBean.getDsl_name() + File.separator, true), layerBean.getLayerAttr().get(StorageTypeKey.prncipal_name));
    }

    public ConfigurationOperator(String conf_dir, String principle_name) {
        PRNCIPAL_NAME = principle_name;
        confDir = conf_dir;
        PATH_TO_KEYTAB = conf_dir + StorageTypeKey.keytab_file;
        PATH_TO_KRB5_CONF = conf_dir + StorageTypeKey.krb5_conf;
        PATH_TO_JAAS = conf_dir + "jaas.conf";
        PATH_TO_CORE_SITE_XML = conf_dir + StorageTypeKey.core_site;
        PATH_TO_HDFS_SITE_XML = conf_dir + StorageTypeKey.hdfs_site;
        PATH_TO_HBASE_SITE_XML = conf_dir + StorageTypeKey.hbase_site;
        PATH_TO_MAPRED_SITE_XML = conf_dir + StorageTypeKey.mapred_site;
        PATH_TO_YARN_SITE_XML = conf_dir + StorageTypeKey.yarn_site;
        log.info("Use " + conf_dir + " configuration file!");
    }

    public Configuration getConfiguration() {
        Configuration conf = loadConf();
        if ((PATH_TO_KEYTAB == null) || (PATH_TO_KEYTAB.length() <= 0)) {
            log.error("input keytabPath is invalid.");
            throw new BusinessException("input keytabPath is invalid.");
        }
        if ((PATH_TO_KRB5_CONF == null) || (PATH_TO_KRB5_CONF.length() <= 0)) {
            log.error("input krb5ConfPath is invalid.");
            throw new BusinessException("input krb5ConfPath is invalid.");
        }
        File userKeytabFile = new File(PATH_TO_KEYTAB);
        if (!userKeytabFile.exists()) {
            log.error("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
            throw new BusinessException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
        }
        if (!userKeytabFile.isFile()) {
            log.error("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") is not a file.");
            throw new BusinessException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") is not a file.");
        }
        File krb5ConfFile = new File(PATH_TO_KRB5_CONF);
        if (!krb5ConfFile.exists()) {
            log.error("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") does not exsit.");
            throw new BusinessException("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") does not exsit.");
        }
        if (!krb5ConfFile.isFile()) {
            log.error("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") is not a file.");
            throw new BusinessException("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") is not a file.");
        }
        setKrb5Config(krb5ConfFile.getAbsolutePath());
        setConfiguration(conf);
        setZookeeperServerPrincipal();
        return conf;
    }

    private Configuration loadConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.addResource(new Path(PATH_TO_CORE_SITE_XML));
        conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        conf.addResource(new Path(PATH_TO_HBASE_SITE_XML));
        conf.addResource(new Path(PATH_TO_MAPRED_SITE_XML));
        conf.addResource(new Path(PATH_TO_YARN_SITE_XML));
        return conf;
    }

    private void setKrb5Config(String krb5ConfFile) {
        System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, krb5ConfFile);
        String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF_KEY);
        if (ret == null) {
            log.error(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
            throw new BusinessException(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
        }
        if (!ret.equals(krb5ConfFile)) {
            log.error(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfFile + ".");
            throw new BusinessException(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfFile + ".");
        }
    }

    private void setConfiguration(Configuration conf) {
        UserGroupInformation.setConfiguration(conf);
    }

    private void setZookeeperServerPrincipal() {
        String zkServerPrincipalKey = ZOOKEEPER_SERVER_PRINCIPAL_KEY;
        System.setProperty(zkServerPrincipalKey, ConfigurationOperator.ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        String ret = System.getProperty(zkServerPrincipalKey);
        if (ret == null) {
            log.error(zkServerPrincipalKey + " is null.");
            throw new BusinessException(zkServerPrincipalKey + " is null.");
        }
        if (!ret.equals(ConfigurationOperator.ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL)) {
            log.error(zkServerPrincipalKey + " is " + ret + " is not " + ConfigurationOperator.ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL + ".");
            throw new BusinessException(zkServerPrincipalKey + " is " + ret + " is not " + ConfigurationOperator.ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL + ".");
        }
    }

    private void loginHbase(Configuration conf, File userKeytabFile) {
        System.setProperty("java.security.auth.login.config", confDir + "jaas.conf");
        if (User.isHBaseSecurityEnabled(conf)) {
            setJaasConf(userKeytabFile);
            setZookeeperServerPrincipal();
        }
    }

    private void setJaasConf(File userKeytabFile) {
        javax.security.auth.login.Configuration.setConfiguration(new ZKSignerSecretProvider.JaasConfiguration(ConfigurationOperator.ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, PRNCIPAL_NAME, userKeytabFile.getAbsolutePath()));
        javax.security.auth.login.Configuration conf = javax.security.auth.login.Configuration.getConfiguration();
        if (!(conf instanceof ZKSignerSecretProvider.JaasConfiguration)) {
            log.error("javax.security.auth.login.Configuration is not JaasConfiguration.");
            throw new AppSystemException("javax.security.auth.login.Configuration is not JaasConfiguration.");
        }
        AppConfigurationEntry[] entrys = conf.getAppConfigurationEntry(ConfigurationOperator.ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME);
        if (entrys == null) {
            log.error("javax.security.auth.login.Configuration has no AppConfigurationEntry named " + ConfigurationOperator.ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME + ".");
            throw new AppSystemException("javax.security.auth.login.Configuration has no AppConfigurationEntry named " + ConfigurationOperator.ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME + ".");
        }
        boolean checkPrincipal = false;
        boolean checkKeytab = false;
        for (AppConfigurationEntry entry : entrys) {
            if (entry.getOptions().get("principal").equals(PRNCIPAL_NAME)) {
                checkPrincipal = true;
            }
            if (IS_IBM_JDK) {
                if (entry.getOptions().get("useKeytab").equals(PATH_TO_KEYTAB)) {
                    checkKeytab = true;
                }
            } else {
                if (entry.getOptions().get("keyTab").equals(PATH_TO_KEYTAB)) {
                    checkKeytab = true;
                }
            }
        }
        if (!checkPrincipal) {
            log.error("AppConfigurationEntry named " + ConfigurationOperator.ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME + " does not have principal value of " + PRNCIPAL_NAME + ".");
            throw new AppSystemException("AppConfigurationEntry named " + ConfigurationOperator.ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME + " does not have principal value of " + PRNCIPAL_NAME + ".");
        }
        if (!checkKeytab) {
            log.error("AppConfigurationEntry named " + ConfigurationOperator.ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME + " does not have keyTab value of " + PATH_TO_KEYTAB + ".");
            throw new AppSystemException("AppConfigurationEntry named " + ConfigurationOperator.ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME + " does not have keyTab value of " + PATH_TO_KEYTAB + ".");
        }
    }
}
