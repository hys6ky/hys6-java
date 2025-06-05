package hyren.serv6.b.batchcollection.cdcCollect;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import hyren.serv6.base.exception.AppSystemException;

public class DataBaseUrlUtil {

    private static final String MYSQL_REGEX = "jdbc:mysql://([\\d.]+):(\\d+).*";

    private static final String ORACLE_REGEX = "jdbc:oracle:thin:@([^:/]+):(\\d+).*";

    private static final String DB2_REGEX = "jdbc:db2://([^:/]+):(\\d+).*";

    private static final String SQLSERVER_REGEX = "jdbc:sqlserver://([^:/]+):(\\d+).*";

    private static final String POSTGRESQL_REGEX = "jdbc:postgresql://([^:/]+):(\\d+).*";

    private static final String KINGBASE_REGEX = "jdbc:kingbase://([^:/]+):(\\d+).*";

    private static final String APACHEDERBY_REGEX = "jdbc:derby://([^:/]+):(\\d+).*";

    private static final String GBASE_REGEX = "jdbc:gbase://([^:/]+):(\\d+).*";

    private static final String TERADATA_REGEX = "jdbc:teradata://([^:/]+):(\\d+).*";

    private static final String INFORMATIC_REGEX = "jdbc:informatica://([^:/]+):(\\d+).*";

    private static final String H2_REGEX = "jdbc:h2:tcp://([^:/]+):(\\d+).*";

    private static final String HIVE_REGEX = "jdbc:hive2://([^:/]+):(\\d+).*";

    private static final String KYLIN_REGEX = "jdbc:kylin://([^:/]+):(\\d+).*";

    private static final String SYBASEASE_REGEX = "jdbc:sybase:Tds:([^:/]+):(\\d+).*";

    public static HostPort getHostAndPortByUrl(String url, String type) {
        try {
            switch(type) {
                case "MYSQL":
                case "mysql":
                case "Mysql":
                    return getHostAndPortByMysqlUrl(url);
                case "ORACLE9I":
                case "oracle9i":
                case "Oracle9i":
                case "ORACLE10":
                case "oracle10":
                case "Oracle10":
                case "ORACLE":
                case "oracle":
                case "Oracle":
                    return getHostAndPortByOracleUrl(url);
                case "DB2":
                case "db2":
                case "Db2":
                    return getHostAndPortByDb2Url(url);
                case "SQLSERVER":
                case "sqlserver":
                case "SqlServer":
                case "Sqlserver":
                    return getHostAndPortBySqlServerUrl(url);
                case "POSTGRESQL":
                case "postgresql":
                case "Postgresql":
                    return getHostAndPortByPostgresqlUrl(url);
                case "KINGBASE":
                case "kingbase":
                case "Kingbase":
                    return getIpAdnPortByKingBaseUrl(url);
                case "APACHEDERBY":
                case "apachederby":
                case "Apachederby":
                    return getIpAdnPortByApacheDerbyUrl(url);
                case "GBASE":
                case "gbase":
                case "GBase":
                case "Gbase":
                    return getIpAdnPortByGBaseUrl(url);
                case "TERADATA":
                case "teradata":
                case "TeraData":
                case "Teradata":
                    return getIpAdnPortByTeraDataUrl(url);
                case "INFORMATIC":
                case "informatic":
                case "Informatic":
                    return getIpAdnPortByInformaticUrl(url);
                case "H2":
                case "h2":
                    return getIpAdnPortByH2Url(url);
                case "HIVE":
                case "hive":
                case "Hive":
                    return getIpAdnPortByHiveUrl(url);
                case "KYLIN":
                case "kylin":
                case "KyLin":
                    return getIpAdnPortByKyLinUrl(url);
                case "SYBASEASE":
                case "sybasease":
                case "SybaseASE":
                case "Sybasease":
                    return getIpAdnPortBySybaseASE(url);
                default:
                    throw new AppSystemException("不支持的数据库类型：" + url + type);
            }
        } catch (Exception e) {
            throw new AppSystemException("不支持的数据库链接或类型：" + url + "," + type);
        }
    }

    public static HostPort getHostAndPortByMysqlUrl(String url) {
        Pattern pattern = Pattern.compile(MYSQL_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("mysql url 转换失败：" + url);
    }

    public static HostPort getHostAndPortByOracleUrl(String url) {
        Pattern pattern = Pattern.compile(ORACLE_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("oracle url 转换失败：" + url);
    }

    public static HostPort getHostAndPortByDb2Url(String url) {
        Pattern pattern = Pattern.compile(DB2_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("db2 url 转换失败：" + url);
    }

    public static HostPort getHostAndPortBySqlServerUrl(String url) {
        Pattern pattern = Pattern.compile(SQLSERVER_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("sqlserver url 转换失败：" + url);
    }

    public static HostPort getHostAndPortByPostgresqlUrl(String url) {
        Pattern pattern = Pattern.compile(POSTGRESQL_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("postgresql url 转换失败：" + url);
    }

    public static HostPort getIpAdnPortByKingBaseUrl(String url) {
        Pattern pattern = Pattern.compile(KINGBASE_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("kingbase url 转换失败：" + url);
    }

    public static HostPort getIpAdnPortByApacheDerbyUrl(String url) {
        Pattern pattern = Pattern.compile(APACHEDERBY_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("apachederby url 转换失败：" + url);
    }

    public static HostPort getIpAdnPortByGBaseUrl(String url) {
        Pattern pattern = Pattern.compile(GBASE_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("gbase url 转换失败：" + url);
    }

    public static HostPort getIpAdnPortByTeraDataUrl(String url) {
        Pattern pattern = Pattern.compile(TERADATA_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("teradata url 转换失败：" + url);
    }

    public static HostPort getIpAdnPortByInformaticUrl(String url) {
        Pattern pattern = Pattern.compile(INFORMATIC_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("informatic url 转换失败：" + url);
    }

    public static HostPort getIpAdnPortByH2Url(String url) {
        Pattern pattern = Pattern.compile(H2_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("h2 url 转换失败：" + url);
    }

    public static HostPort getIpAdnPortByHiveUrl(String url) {
        Pattern pattern = Pattern.compile(HIVE_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("hive url 转换失败：" + url);
    }

    public static HostPort getIpAdnPortByKyLinUrl(String url) {
        Pattern pattern = Pattern.compile(KYLIN_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("kylin url 转换失败：" + url);
    }

    public static HostPort getIpAdnPortBySybaseASE(String url) {
        Pattern pattern = Pattern.compile(SYBASEASE_REGEX);
        Matcher matcher = pattern.matcher(url);
        if (matcher.matches()) {
            String ipAddress = matcher.group(1);
            int port = Integer.parseInt(matcher.group(2));
            return new HostPort(ipAddress, port);
        }
        throw new AppSystemException("sybasease url 转换失败：" + url);
    }

    public static class HostPort {

        private String host;

        private Integer port;

        public HostPort(String host, Integer port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return this.host;
        }

        public Integer getPort() {
            return this.port;
        }
    }
}
