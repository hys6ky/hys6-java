package hyren.serv6.hadoop.commons.loginAuth.impl;

import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@Slf4j
public class CDH5LoginAuthImpl extends AbsLoginAuthImpl {

    public enum Module {

        STORM("StormClient"), KAFKA("KafkaClient"), ZOOKEEPER("Client"), SolrJClient("SolrJClient ");

        private final String name;

        Module(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private static final String IBM_LOGIN_MODULE = "com.ibm.security.auth.module.Krb5LoginModule required";

    private static final String SUN_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule required";

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private static final String JAAS_CONF_FILE = "jaas.conf";

    public CDH5LoginAuthImpl() {
        this(System.getProperty("user.dir") + File.separator + "conf" + File.separator);
    }

    public CDH5LoginAuthImpl(String conf_dir) {
        confDir = conf_dir;
        PATH_TO_KEYTAB = conf_dir + StorageTypeKey.keytab_file;
        PATH_TO_KRB5_CONF = conf_dir + StorageTypeKey.krb5_conf;
        PATH_TO_JAAS = conf_dir + "jaas.conf";
        PATH_TO_CORE_SITE_XML = conf_dir + StorageTypeKey.core_site;
        PATH_TO_HDFS_SITE_XML = conf_dir + StorageTypeKey.hdfs_site;
        PATH_TO_HBASE_SITE_XML = conf_dir + StorageTypeKey.hbase_site;
        PATH_TO_MAPRED_SITE_XML = conf_dir + StorageTypeKey.mapred_site;
        PATH_TO_YARN_SITE_XML = conf_dir + StorageTypeKey.yarn_site;
        log.info("platform: CDH5, go to the " + conf_dir + " configuration file!");
    }

    @Override
    public synchronized Configuration login(String principle_name, String keytabPath, String krb5ConfPath, Configuration conf) {
        log.info("principle_name 认证配置文件信息 : " + principle_name);
        log.info("keytabPath 认证配置文件信息 : " + keytabPath);
        log.info("krb5ConfPath 认证配置文件信息 : " + krb5ConfPath);
        log.info("CDH5 认证配置文件信息 : " + conf);
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
        setZookeeperServerPrincipal("zookeeper/hadoop");
        loginHbase(conf, userKeytabFile);
        loginHDFS(userKeytabFile);
        log.info("Login success!!!!!!!!!!!!!!");
        return conf;
    }

    @Override
    public void loginHbase(Configuration conf, File userKeytabFile) {
        if (User.isHBaseSecurityEnabled(conf)) {
            String jaasPath = confDir + File.separator + JAAS_CONF_FILE;
            jaasPath = jaasPath.replace("\\", "\\\\");
            deleteJaasFile(jaasPath);
            writeJaasFile(jaasPath, userKeytabFile.getAbsolutePath());
            System.setProperty("java.security.auth.login.config", confDir + "jaas.conf");
            setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userKeytabFile);
            setZookeeperServerPrincipal(ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        }
    }

    @Override
    protected void setJaasConf(String loginContextName, File userKeytabFile) {
        super.setJaasConf("Client", userKeytabFile);
    }

    private static void writeJaasFile(String jaasPath, String keytabPath) {
        try (FileWriter writer = new FileWriter(jaasPath)) {
            writer.write(getJaasConfContext(keytabPath));
            writer.flush();
        } catch (IOException e) {
            throw new AppSystemException("Failed to create jaas.conf File.");
        }
    }

    private static void deleteJaasFile(String jaasPath) {
        File jaasFile = new File(jaasPath);
        if (jaasFile.exists()) {
            if (!jaasFile.delete()) {
                throw new AppSystemException("Failed to delete exists jaas file.");
            }
        }
    }

    private static String getJaasConfContext(String keytabPath) {
        Module[] allModule = Module.values();
        StringBuilder builder = new StringBuilder();
        for (Module module : allModule) {
            builder.append(getModuleContext(keytabPath, module));
        }
        return builder.toString();
    }

    private static String getModuleContext(String keyTabPath, Module module) {
        StringBuilder builder = new StringBuilder();
        if (IS_IBM_JDK) {
            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(IBM_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("credsType=both").append(LINE_SEPARATOR);
            builder.append("principal=\"").append(PRNCIPAL_NAME).append("\"").append(LINE_SEPARATOR);
            builder.append("useKeytab=\"").append(keyTabPath).append("\"").append(LINE_SEPARATOR);
            builder.append("debug=false;").append(LINE_SEPARATOR);
            builder.append("};").append(LINE_SEPARATOR);
        } else {
            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(SUN_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("useKeyTab=true").append(LINE_SEPARATOR);
            builder.append("keyTab=\"").append(keyTabPath).append("\"").append(LINE_SEPARATOR);
            builder.append("principal=\"").append(PRNCIPAL_NAME).append("\"").append(LINE_SEPARATOR);
            builder.append("useTicketCache=true").append(LINE_SEPARATOR);
            builder.append("storeKey=true").append(LINE_SEPARATOR);
            builder.append("debug=false;").append(LINE_SEPARATOR);
            builder.append("};").append(LINE_SEPARATOR);
        }
        return builder.toString();
    }
}
