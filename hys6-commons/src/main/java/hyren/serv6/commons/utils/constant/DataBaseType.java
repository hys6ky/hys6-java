package hyren.serv6.commons.utils.constant;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.database.bean.DBConnectionProp;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public enum DataBaseType {

    MYSQL("MYSQL") {

        @Override
        public String getDriver() {
            return "com.mysql.jdbc.Driver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:mysql://");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder("/");
            template.setUrlSuffix("?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull");
            template.setDbDriver(MYSQL.getDriver());
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.MYSQL;
        }
    }
    ,
    Oracle10("Oracle10") {

        @Override
        public String getDriver() {
            return "oracle.jdbc.OracleDriver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:oracle:thin:@");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder(":");
            template.setUrlSuffix("");
            template.setDbDriver("oracle.jdbc.OracleDriver");
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.ORACLE;
        }

        @Override
        public List<String> getExternalTableKeys() {
            return new ArrayList<>(Arrays.asList(StorageTypeKey.database_driver, StorageTypeKey.jdbc_url, StorageTypeKey.user_name, StorageTypeKey.database_pwd, StorageTypeKey.database_type, StorageTypeKey.database_name, StorageTypeKey.sftp_host, StorageTypeKey.sftp_port, StorageTypeKey.sftp_pwd, StorageTypeKey.sftp_user, StorageTypeKey.external_root_path, StorageTypeKey.database_code, StorageTypeKey.external_directory));
        }
    }
    ,
    ORACLE9I("ORACLE9I") {

        @Override
        public String getDriver() {
            return "oracle.jdbc.driver.OracleDriver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:oracle:thin:@");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder(":");
            template.setUrlSuffix("");
            template.setDbDriver(ORACLE9I.getDriver());
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.ORACLE;
        }

        public List<String> getExternalTableKeys() {
            return Oracle10.getExternalTableKeys();
        }
    }
    ,
    SqlServer("SQLSERVER") {

        @Override
        public String getDriver() {
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:sqlserver://");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder(";DatabaseName=");
            template.setUrlSuffix("");
            template.setDbDriver(SqlServer.getDriver());
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.SQLSERVER;
        }
    }
    ,
    DB2("DB2") {

        @Override
        public String getDriver() {
            return "com.ibm.db2.jcc.DB2Driver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:db2://");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder("/");
            template.setUrlSuffix("");
            template.setDbDriver(DB2.getDriver());
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.DB2V1;
        }
    }
    ,
    SybaseASE("SybaseASE") {

        @Override
        public String getDriver() {
            return "com.sybase.jdbc2.jdbc.SybDriver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:sybase:Tds:");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder("/");
            template.setUrlSuffix("");
            template.setDbDriver(SybaseASE.getDriver());
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.SYBASE;
        }
    }
    ,
    Informatic("Informatic") {

        @Override
        public String getDriver() {
            return "com.informix.jdbc.IfxDriver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:informix-sqli://");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder("/");
            template.setUrlSuffix(":INFORMIXSERVER=myserver");
            template.setDbDriver(Informatic.getDriver());
            return template;
        }
    }
    ,
    H2("H2") {

        @Override
        public String getDriver() {
            return "org.h2.Driver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:h2:tcp://");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder("/");
            template.setUrlSuffix("");
            template.setDbDriver(H2.getDriver());
            return template;
        }
    }
    ,
    ApacheDerby("ApacheDerby") {

        @Override
        public String getDriver() {
            return "org.apache.derby.jdbc.EmbeddedDriver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:derby://");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder("/");
            template.setUrlSuffix(";create=true");
            template.setDbDriver(ApacheDerby.getDriver());
            return template;
        }
    }
    ,
    Postgresql("Postgresql") {

        @Override
        public String getDriver() {
            return "org.postgresql.Driver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:postgresql://");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder("/");
            template.setUrlSuffix("");
            template.setDbDriver("org.postgresql.Driver");
            template.setDbDriver(Postgresql.getDriver());
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.POSTGRESQL;
        }

        @Override
        public List<String> getExternalTableKeys() {
            return new ArrayList<>(Arrays.asList(StorageTypeKey.database_driver, StorageTypeKey.jdbc_url, StorageTypeKey.user_name, StorageTypeKey.database_pwd, StorageTypeKey.database_type, StorageTypeKey.database_name, StorageTypeKey.sftp_host, StorageTypeKey.sftp_port, StorageTypeKey.sftp_pwd, StorageTypeKey.sftp_user, StorageTypeKey.external_root_path, StorageTypeKey.database_code));
        }
    }
    ,
    GBase("GBase") {

        @Override
        public String getDriver() {
            return "com.gbase.jdbc.Driver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:gbase://");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder("/");
            template.setUrlSuffix("");
            template.setDbDriver(GBase.getDriver());
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.GBASE;
        }
    }
    ,
    TeraData("TeraData") {

        @Override
        public String getDriver() {
            return "com.ncr.teradata.TeraDriver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:teradata://");
            template.setIpPlaceholder("/TMODE=TERA,CHARSET=ASCII,CLIENT_CHARSET=cp936,DATABASE=");
            template.setPortPlaceholder("");
            if (StringUtil.isBlank(port)) {
                template.setUrlSuffix(",lob_support=off");
            } else {
                template.setUrlSuffix(",lob_support=off,DBS_PORT=");
            }
            template.setDbDriver(TeraData.getDriver());
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.TERADATA;
        }
    }
    ,
    Hive("Hive") {

        @Override
        public String getDriver() {
            return "org.apache.hive.jdbc.HiveDriver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:hive2://");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder("/");
            template.setUrlSuffix("");
            template.setDbDriver(Hive.getDriver());
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.HIVE;
        }
    }
    ,
    Odps("Odps") {

        @Override
        public String getDriver() {
            return "com.aliyun.odps.jdbc.OdpsDriver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:odps:");
            template.setIpPlaceholder("");
            template.setPortPlaceholder("");
            template.setUrlSuffix("");
            template.setDbDriver(Odps.getDriver());
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.ODPS;
        }
    }
    ,
    KingBase("KingBase") {

        @Override
        public String getDriver() {
            return "com.kingbase8.Driver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:kingbase8://");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder("/");
            template.setUrlSuffix("");
            template.setDbDriver(KingBase.getDriver());
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.KINGBASE;
        }

        public List<String> getExternalTableKeys() {
            return Oracle10.getExternalTableKeys();
        }
    }
    ,
    KyLin("KyLin") {

        @Override
        public String getDriver() {
            return "org.apache.kylin.jdbc.Driver";
        }

        @Override
        public DBConnectionProp getJdbcUrl(String port) {
            DBConnectionProp template = new DBConnectionProp();
            template.setUrlPrefix("jdbc:kylin://");
            template.setIpPlaceholder(":");
            template.setPortPlaceholder("/");
            template.setUrlSuffix("");
            template.setDbDriver(KyLin.getDriver());
            return template;
        }

        @Override
        public Dbtype getDbtype() {
            return Dbtype.KYLIN;
        }
    }
    ,
    ANYONE("anyone");

    private final String desc;

    DataBaseType(String desc) {
        this.desc = desc;
    }

    public String getDesc() {
        return desc;
    }

    public String getDriver() {
        throw new AppSystemException("系统不支持数据库类型:" + desc);
    }

    public DBConnectionProp getJdbcUrl(String port) {
        throw new AppSystemException("系统不支持数据库类型:" + desc);
    }

    public Dbtype getDbtype() {
        throw new AppSystemException("系统不支持数据库类型:" + desc);
    }

    public List<String> getExternalTableKeys() {
        return null;
    }

    public static DataBaseType getDatabase(String dbType) {
        dbType = dbType.toUpperCase();
        if (dbType.contains("MYSQL"))
            return DataBaseType.MYSQL;
        else if (dbType.contains("ORACLE9I"))
            return DataBaseType.ORACLE9I;
        else if (dbType.contains("ORACLE10G"))
            return DataBaseType.Oracle10;
        else if (dbType.contains("DB2"))
            return DataBaseType.DB2;
        else if (dbType.contains("SQLSERVER"))
            return DataBaseType.SqlServer;
        else if (dbType.contains("POSTGRESQL"))
            return DataBaseType.Postgresql;
        else if (dbType.contains("KINGBASE"))
            return DataBaseType.KingBase;
        else if (dbType.contains("APACHEDERBY"))
            return DataBaseType.ApacheDerby;
        else if (dbType.contains("GBASE"))
            return DataBaseType.GBase;
        else if (dbType.contains("TERADATA"))
            return DataBaseType.TeraData;
        else if (dbType.contains("INFORMATIC"))
            return DataBaseType.Informatic;
        else if (dbType.contains("H2"))
            return DataBaseType.H2;
        else if (dbType.contains("HIVE"))
            return DataBaseType.Hive;
        else if (dbType.contains("ODPS"))
            return DataBaseType.Odps;
        else if (dbType.contains("KYLIN"))
            return DataBaseType.KyLin;
        else if (dbType.contains("SYBASEASE"))
            return DataBaseType.SybaseASE;
        else
            throw new AppSystemException(String.format("目前不支持对该数据库类型 %s 进行采集，请联系管理员", dbType));
    }
}
