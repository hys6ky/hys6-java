package hyren.serv6.agent.trans.biz.database;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.base.entity.TableColumn;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.packutil.PackUtil;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.bean.JDBCBean;
import hyren.serv6.commons.utils.xlstoxml.Platform;
import hyren.serv6.commons.utils.xlstoxml.Xls2xml;
import hyren.serv6.commons.utils.xlstoxml.util.ConnUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/10/15 14:40")
public class DatabaseInfoService {

    @Method(desc = "", logicStep = "")
    @Param(name = "database_set", desc = "", range = "", isBean = true)
    @Param(name = "search", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public String getDatabaseTable(SourceDataConfBean database_set, String search) {
        List<Object> table_List;
        if (IsFlag.Shi.getCode().equals(database_set.getDb_agent())) {
            String db_path = database_set.getPlane_url();
            String xmlName = Math.abs(db_path.hashCode()) + ".xml";
            Xls2xml.toXml(db_path, xmlName);
            table_List = ConnUtil.getTableToXML(xmlName);
        } else {
            String jdbcUrl = database_set.getJdbc_url();
            URI uri = null;
            try {
                uri = new URI(jdbcUrl.substring(5));
            } catch (URISyntaxException e) {
                throw new BusinessException("jdbc格式有误", e.getStackTrace());
            }
            String ipAddress = uri.getHost();
            String port = String.valueOf(uri.getPort());
            String database_name = database_set.getDatabase_name();
            String user_name = database_set.getUser_name();
            String xmlName = ConnUtil.getDataBaseFile(ipAddress, port, database_name, user_name);
            JDBCBean bean = new JDBCBean();
            bean.setDatabase_drive(database_set.getDatabase_drive());
            bean.setDatabase_name(database_name);
            bean.setDatabase_type(database_set.getDatabase_type());
            bean.setJdbc_url(database_set.getJdbc_url());
            bean.setUser_name(user_name);
            bean.setFetch_size(database_set.getFetch_size());
            bean.setDatabase_pad(database_set.getDatabase_pad());
            try (DatabaseWrapper db = ConnectionTool.getDBWrapper(bean)) {
                if (StringUtil.isEmpty(search)) {
                    Platform.readModelFromDatabase(db, xmlName);
                } else {
                    Platform.readModelFromDatabase(db, xmlName, search);
                }
                table_List = ConnUtil.getTable(xmlName, search);
            } catch (Exception e) {
                throw new BusinessException("获取数据库的表信息失败" + e.getMessage());
            }
        }
        return PackUtil.packMsg(JsonUtil.toJson(table_List));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "database_set", desc = "", range = "", isBean = true)
    @Param(name = "tableName", desc = "", range = "", nullable = true)
    @Param(name = "hy_sql_meta", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public String getTableColumn(SourceDataConfBean database_set, String tableName, String hy_sql_meta) {
        List<Map<String, String>> columnList;
        if (IsFlag.Shi.getCode().equals(database_set.getDb_agent())) {
            String db_path = database_set.getPlane_url();
            String xmlName = ConnUtil.getDataBaseFile("", "", db_path, "");
            try {
                columnList = ConnUtil.getColumnByTable(xmlName, tableName);
            } catch (Exception e) {
                Xls2xml.toXml(db_path, xmlName);
                columnList = ConnUtil.getColumnByTable(xmlName, tableName);
            }
        } else {
            String jdbcUrl = database_set.getJdbc_url();
            URI uri = null;
            try {
                uri = new URI(jdbcUrl.substring(5));
            } catch (URISyntaxException e) {
                throw new BusinessException("jdbc格式有误", e.getStackTrace());
            }
            String database_name = database_set.getDatabase_name();
            String user_name = database_set.getUser_name();
            //数据库IP
            String database_ip = uri.getHost();
            ;
            String database_port = String.valueOf(uri.getPort());
            String xmlName = ConnUtil.getDataBaseFile(database_ip, database_port, database_name, user_name);
            JDBCBean bean = new JDBCBean();
            bean.setDatabase_drive(database_set.getDatabase_drive());
            bean.setDatabase_name(database_name);
            bean.setDatabase_type(database_set.getDatabase_type());
            bean.setJdbc_url(database_set.getJdbc_url());
            bean.setUser_name(user_name);
            bean.setFetch_size(database_set.getFetch_size());
            bean.setDatabase_pad(database_set.getDatabase_pad());
            try (DatabaseWrapper db = ConnectionTool.getDBWrapper(bean)) {
                if (!StringUtil.isEmpty(hy_sql_meta)) {
                    columnList = Platform.getSqlColumnMeta(hy_sql_meta, db);
                } else {
                    try {
                        columnList = ConnUtil.getColumnByTable(xmlName, tableName);
                    } catch (Exception e) {
                        Platform.readModelFromDatabase(db, xmlName, tableName);
                        columnList = ConnUtil.getColumnByTable(xmlName, tableName);
                    }
                }
            } catch (Exception e) {
                throw new BusinessException("获取表字段异常");
            }
        }
        return PackUtil.packMsg(JsonUtil.toJson(columnList));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "database_set", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public String getAllTableColumn(DatabaseSet database_set) {
        Map<String, List<Map<String, String>>> columnList;
        String db_path = database_set.getPlane_url();
        String xmlName = ConnUtil.getDataBaseFile("", "", db_path, "");
        try {
            columnList = ConnUtil.getColumnByXml(xmlName);
        } catch (Exception e) {
            Xls2xml.toXml(db_path, xmlName);
            columnList = ConnUtil.getColumnByXml(xmlName);
        }
        return PackUtil.packMsg(JsonUtil.toJson(columnList));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "database_set", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public String getAllTableStorage(DatabaseSet database_set) {
        List<Map<String, Object>> allStorageList;
        String db_path = database_set.getPlane_url();
        String xmlName = ConnUtil.getDataBaseFile("", "", db_path, "");
        try {
            allStorageList = ConnUtil.getStorageByXml(xmlName);
        } catch (Exception e) {
            Xls2xml.toXml(db_path, xmlName);
            allStorageList = ConnUtil.getStorageByXml(xmlName);
        }
        return PackUtil.packMsg(JsonUtil.toJson(allStorageList));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dbSet", desc = "", range = "", isBean = true)
    @Param(name = "custSQL", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getCustColumn(SourceDataConfBean dbSet, String custSQL) {
        custSQL = custSQL.replaceAll("\\s*?((?!\\s).)+?\\s*?=\\s*?#\\{.*}", " 1=2");
        JDBCBean bean = new JDBCBean();
        bean.setDatabase_drive(dbSet.getDatabase_drive());
        bean.setDatabase_name(dbSet.getDatabase_name());
        bean.setDatabase_type(dbSet.getDatabase_type());
        bean.setJdbc_url(dbSet.getJdbc_url());
        bean.setUser_name(dbSet.getUser_name());
        bean.setFetch_size(dbSet.getFetch_size());
        bean.setDatabase_pad(dbSet.getDatabase_pad());
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(bean)) {
            if (db.getDbtype() == Dbtype.KINGBASE) {
                custSQL = custSQL.replaceAll("#\\{.*\\}", "''");
            } else {
                custSQL = custSQL.replaceAll("'#\\{.*\\}'", "' '");
            }
            ResultSet rs = db.queryGetResultSet(custSQL);
            List<TableColumn> tableColumns = new ArrayList<>();
            try {
                ResultSetMetaData metaData = rs.getMetaData();
                for (int j = 1; j <= metaData.getColumnCount(); j++) {
                    TableColumn tableColumn = new TableColumn();
                    tableColumn.setColumn_name(metaData.getColumnName(j));
                    String colTypeAndPreci = Platform.getColType(metaData.getColumnType(j), metaData.getColumnTypeName(j), metaData.getPrecision(j), metaData.getScale(j), metaData.getColumnDisplaySize(j));
                    tableColumn.setColumn_type(colTypeAndPreci);
                    tableColumn.setColumn_ch_name(metaData.getColumnName(j));
                    tableColumns.add(tableColumn);
                }
            } catch (SQLException e) {
                throw new BusinessException("获取自定义SQL抽取列信息失败" + e.getMessage());
            }
            return PackUtil.packMsg(JsonUtil.toJson(tableColumns));
        }
    }
}
