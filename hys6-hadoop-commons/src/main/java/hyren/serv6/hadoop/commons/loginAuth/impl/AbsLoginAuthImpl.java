package hyren.serv6.hadoop.commons.loginAuth.impl;

import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.hadoop.commons.loginAuth.ILoginAuth;
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
public abstract class AbsLoginAuthImpl implements ILoginAuth {

    protected static String confDir;

    protected static Configuration configuration;

    protected static String PRNCIPAL_NAME = "admin@HADOOP.COM";

    protected static String PATH_TO_KEYTAB;

    protected static String PATH_TO_KRB5_CONF;

    protected static String PATH_TO_JAAS;

    protected static String PATH_TO_CORE_SITE_XML;

    protected static String PATH_TO_HDFS_SITE_XML;

    protected static String PATH_TO_HBASE_SITE_XML;

    protected static String PATH_TO_MAPRED_SITE_XML;

    protected static String PATH_TO_YARN_SITE_XML;

    protected static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

    protected static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    protected static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";

    protected static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    protected static final String LOGIN_FAILED_CAUSE_PASSWORD_WRONG = "(wrong password) keytab file and user not match, you can kinit -k -t keytab user in client server to check";

    protected static final String LOGIN_FAILED_CAUSE_TIME_WRONG = "(clock skew) time of local server and remote server not match, please check ntp to remote server";

    protected static final String LOGIN_FAILED_CAUSE_AES256_WRONG = "(aes256 not support) aes256 not support by default jdk/jre, need copy local_policy.jar and US_export_policy.jar from remote server in path /opt/huawei/Bigdata/jdk/jre/lib/security";

    protected static final String LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG = "(no rule) principal format not support by default, need add property hadoop.security.auth_to_local(in core-site.xml) value RULE:[1:$1] RULE:[2:$1]";

    protected static final String LOGIN_FAILED_CAUSE_TIME_OUT = "(time out) can not connect to kdc server or there is fire wall in the network";

    protected static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");

    public AbsLoginAuthImpl() {
    }

    public synchronized Configuration login(String principle_name) {
        return login(principle_name, PATH_TO_KEYTAB, PATH_TO_KRB5_CONF, loadConf());
    }

    public synchronized Configuration login(String principle_name, String keytabPath, String krb5ConfPath) {
        return login(principle_name, PATH_TO_KEYTAB, PATH_TO_KRB5_CONF, configuration);
    }

    public synchronized Configuration login(String principle_name, String keytabPath, String krb5ConfPath, Configuration conf) {
        PRNCIPAL_NAME = principle_name;
        if ((principle_name == null) || (principle_name.length() <= 0)) {
            log.error("input principle_name is invalid.");
            throw new BusinessException("input principle_name is invalid.");
        }
        if ((keytabPath == null) || (keytabPath.length() <= 0)) {
            log.error("input keytabPath is invalid.");
            throw new BusinessException("input keytabPath is invalid.");
        }
        if ((krb5ConfPath == null) || (krb5ConfPath.length() <= 0)) {
            log.error("input krb5ConfPath is invalid.");
            throw new BusinessException("input krb5ConfPath is invalid.");
        }
        if ((conf == null)) {
            log.error("input conf is invalid.");
            throw new BusinessException("input conf is invalid.");
        }
        File userKeytabFile = new File(keytabPath);
        if (!userKeytabFile.exists()) {
            log.error("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
            throw new BusinessException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
        }
        if (!userKeytabFile.isFile()) {
            log.error("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") is not a file.");
            throw new BusinessException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") is not a file.");
        }
        File krb5ConfFile = new File(krb5ConfPath);
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
        setZookeeperServerPrincipal(ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        loginHbase(conf, userKeytabFile);
        loginHDFS(userKeytabFile);
        log.info("Login success!!!!!!!!!!!!!!");
        return conf;
    }

    protected void setKrb5Config(String krb5ConfFile) {
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

    protected void loginHbase(Configuration conf, File userKeytabFile) {
        System.setProperty("java.security.auth.login.config", confDir + "jaas.conf");
        if (User.isHBaseSecurityEnabled(conf)) {
            setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userKeytabFile);
            setZookeeperServerPrincipal(ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        }
    }

    protected void setJaasConf(String loginContextName, File userKeytabFile) {
        if ((loginContextName == null) || (loginContextName.length() <= 0)) {
            log.error("input loginContextName is invalid.");
            throw new AppSystemException("input loginContextName is invalid.");
        }
        javax.security.auth.login.Configuration.setConfiguration(new ZKSignerSecretProvider.JaasConfiguration(loginContextName, PRNCIPAL_NAME, userKeytabFile.getAbsolutePath()));
        javax.security.auth.login.Configuration conf = javax.security.auth.login.Configuration.getConfiguration();
        if (!(conf instanceof ZKSignerSecretProvider.JaasConfiguration)) {
            log.error("javax.security.auth.login.Configuration is not JaasConfiguration.");
            throw new AppSystemException("javax.security.auth.login.Configuration is not JaasConfiguration.");
        }
        AppConfigurationEntry[] entrys = conf.getAppConfigurationEntry(loginContextName);
        if (entrys == null) {
            log.error("javax.security.auth.login.Configuration has no AppConfigurationEntry named " + loginContextName + ".");
            throw new AppSystemException("javax.security.auth.login.Configuration has no AppConfigurationEntry named " + loginContextName + ".");
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
            log.error("AppConfigurationEntry named " + loginContextName + " does not have principal value of " + PRNCIPAL_NAME + ".");
            throw new AppSystemException("AppConfigurationEntry named " + loginContextName + " does not have principal value of " + PRNCIPAL_NAME + ".");
        }
        if (!checkKeytab) {
            log.error("AppConfigurationEntry named " + loginContextName + " does not have keyTab value of " + PATH_TO_KEYTAB + ".");
            throw new AppSystemException("AppConfigurationEntry named " + loginContextName + " does not have keyTab value of " + PATH_TO_KEYTAB + ".");
        }
    }

    protected void setConfiguration(Configuration conf) {
        UserGroupInformation.setConfiguration(conf);
    }

    protected void setZookeeperServerPrincipal(String zkServerPrincipal) {
        String zkServerPrincipalKey = ZOOKEEPER_SERVER_PRINCIPAL_KEY;
        System.setProperty(zkServerPrincipalKey, zkServerPrincipal);
        String ret = System.getProperty(zkServerPrincipalKey);
        if (ret == null) {
            log.error(zkServerPrincipalKey + " is null.");
            throw new BusinessException(zkServerPrincipalKey + " is null.");
        }
        if (!ret.equals(zkServerPrincipal)) {
            log.error(zkServerPrincipalKey + " is " + ret + " is not " + zkServerPrincipal + ".");
            throw new BusinessException(zkServerPrincipalKey + " is " + ret + " is not " + zkServerPrincipal + ".");
        }
    }

    protected void loginHDFS(File userKeytabFile) {
        try {
            UserGroupInformation.loginUserFromKeytab(PRNCIPAL_NAME, userKeytabFile.getAbsolutePath());
        } catch (Exception ioe) {
            log.error("login failed with " + PRNCIPAL_NAME + " and " + userKeytabFile.getAbsolutePath() + ".");
            log.error("perhaps cause 1 is " + LOGIN_FAILED_CAUSE_PASSWORD_WRONG + ".");
            log.error("perhaps cause 2 is " + LOGIN_FAILED_CAUSE_TIME_WRONG + ".");
            log.error("perhaps cause 3 is " + LOGIN_FAILED_CAUSE_AES256_WRONG + ".");
            log.error("perhaps cause 4 is " + LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG + ".");
            log.error("perhaps cause 5 is " + LOGIN_FAILED_CAUSE_TIME_OUT + ".");
            ioe.printStackTrace();
            log.error("======" + ioe);
            throw new AppSystemException("keytab file authentication failed!" + ioe);
        }
    }

    protected Configuration loadConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.addResource(new Path(PATH_TO_CORE_SITE_XML));
        conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        conf.addResource(new Path(PATH_TO_HBASE_SITE_XML));
        conf.addResource(new Path(PATH_TO_MAPRED_SITE_XML));
        conf.addResource(new Path(PATH_TO_YARN_SITE_XML));
        return conf;
    }
}
