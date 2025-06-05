package hyren.serv6.commons.solr.login;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import lombok.Getter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/1/14 0014 上午 11:32")
public class SolrLogin {

    @Getter
    public enum Module {

        STORM("StormClient"), KAFKA("KafkaClient"), ZOOKEEPER("Client"), SolrJClient("SolrJClient ");

        private final String name;

        Module(String name) {
            this.name = name;
        }
    }

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static final String JAAS_POSTFIX = ".jaas.conf";

    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");

    private static final String IBM_LOGIN_MODULE = "com.ibm.security.auth.module.Krb5LoginModule required";

    private static final String SUN_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule required";

    public static final String ZOOKEEPER_AUTH_PRINCIPAL = "zookeeper.server.principal";

    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    public static final String JAVA_SECURITY_LOGIN_CONF = "java.security.auth.login.config";

    private static String PRINCIPLE_NAME = "hyshf@beyondsoft.com";

    @Method(desc = "", logicStep = "")
    @Param(name = "keyTabPath", desc = "", range = "")
    public static void setJaasFile(String principle_name, String keyTabPath) {
        if (StringUtil.isNotBlank(principle_name)) {
            PRINCIPLE_NAME = principle_name;
        }
        String jaasPath = new File(System.getProperty("java.io.tmpdir")) + File.separator + System.getProperty("user.name") + JAAS_POSTFIX;
        jaasPath = jaasPath.replace("\\", "\\\\");
        deleteJaasFile(jaasPath);
        writeJaasFile(jaasPath, keyTabPath);
        System.setProperty(JAVA_SECURITY_LOGIN_CONF, jaasPath);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "zkServerPrincipal", desc = "", range = "")
    public static void setZookeeperServerPrincipal(String zkServerPrincipal) {
        System.setProperty(ZOOKEEPER_AUTH_PRINCIPAL, zkServerPrincipal);
        String ret = System.getProperty(ZOOKEEPER_AUTH_PRINCIPAL);
        if (ret == null) {
            throw new AppSystemException(ZOOKEEPER_AUTH_PRINCIPAL + " is null.");
        }
        if (!ret.equals(zkServerPrincipal)) {
            throw new AppSystemException(ZOOKEEPER_AUTH_PRINCIPAL + " is " + ret + " is not " + zkServerPrincipal + ".");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "krb5ConfFile", desc = "", range = "")
    public static void setKrb5Config(String krb5ConfFile) {
        System.setProperty(JAVA_SECURITY_KRB5_CONF, krb5ConfFile);
        String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF);
        if (ret == null) {
            throw new AppSystemException(JAVA_SECURITY_KRB5_CONF + " is null.");
        }
        if (!ret.equals(krb5ConfFile)) {
            throw new AppSystemException(JAVA_SECURITY_KRB5_CONF + " is " + ret + " is not " + krb5ConfFile + ".");
        }
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
            builder.append("principal=\"").append(PRINCIPLE_NAME).append("\"").append(LINE_SEPARATOR);
            builder.append("useKeytab=\"").append(keyTabPath).append("\"").append(LINE_SEPARATOR);
            builder.append("debug=false;").append(LINE_SEPARATOR);
            builder.append("};").append(LINE_SEPARATOR);
        } else {
            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(SUN_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("useKeyTab=true").append(LINE_SEPARATOR);
            builder.append("keyTab=\"").append(keyTabPath).append("\"").append(LINE_SEPARATOR);
            builder.append("principal=\"").append(PRINCIPLE_NAME).append("\"").append(LINE_SEPARATOR);
            builder.append("useTicketCache=true").append(LINE_SEPARATOR);
            builder.append("storeKey=true").append(LINE_SEPARATOR);
            builder.append("debug=false;").append(LINE_SEPARATOR);
            builder.append("};").append(LINE_SEPARATOR);
        }
        return builder.toString();
    }
}
