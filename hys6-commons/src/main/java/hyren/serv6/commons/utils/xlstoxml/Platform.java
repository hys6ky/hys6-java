package hyren.serv6.commons.utils.xlstoxml;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.meta.ColumnMeta;
import fd.ng.db.meta.MetaOperator;
import fd.ng.db.meta.TableMeta;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.w3c.dom.Element;
import java.io.File;
import java.sql.Types;
import java.util.*;

@Slf4j
public class Platform {

    private static XmlCreater xmlCreater = null;

    public static Element table = null;

    public static Element root = null;

    public static Element column = null;

    private static void createXml(String path, String name) {
        xmlCreater = new XmlCreater(path);
        root = xmlCreater.createRootElement("database");
        xmlCreater.createAttribute(root, "xmlns", "http://db.apache.org/ddlutils/schema/1.1");
        xmlCreater.createAttribute(root, "name", name);
    }

    private static void openXml(String path) {
        xmlCreater = new XmlCreater(path);
        xmlCreater.open();
        root = xmlCreater.getElement();
    }

    private static void addTable(String en_table_name, String cn_table_name, TableMeta tableMeta) {
        table = xmlCreater.createElement(root, "table");
        xmlCreater.createAttribute(table, "table_name", en_table_name);
        xmlCreater.createAttribute(table, "table_ch_name", StringUtil.isEmpty(cn_table_name) ? en_table_name : cn_table_name);
        Map<String, ColumnMeta> columnMetas = tableMeta.getColumnMetas();
        Set<String> primaryKeys = tableMeta.getPrimaryKeys();
        for (String key : columnMetas.keySet()) {
            ColumnMeta columnMeta = columnMetas.get(key);
            String colName = columnMeta.getName();
            String colCnName = columnMeta.getRemark();
            String typeName = columnMeta.getTypeName();
            int precision = columnMeta.getLength();
            int dataType = columnMeta.getTypeOfSQL();
            int scale = columnMeta.getScale();
            String column_type = getColType(dataType, typeName, precision, scale, 0);
            boolean primaryKey = primaryKeys.contains(colName);
            addColumn(colName, colCnName, primaryKey, column_type);
        }
    }

    private static void addColumn(String colName, String colCnName, boolean primaryKey, String column_type) {
        column = xmlCreater.createElement(table, "column");
        xmlCreater.createAttribute(column, "column_name", colName);
        xmlCreater.createAttribute(column, "column_type", column_type);
        xmlCreater.createAttribute(column, "column_ch_name", StringUtil.isBlank(colCnName) ? colName : colCnName);
        xmlCreater.createAttribute(column, "is_primary_key", primaryKey ? IsFlag.Shi.getCode() : IsFlag.Fou.getCode());
        xmlCreater.createAttribute(column, "is_get", IsFlag.Shi.getCode());
        xmlCreater.createAttribute(column, "is_alive", IsFlag.Shi.getCode());
        xmlCreater.createAttribute(column, "is_new", IsFlag.Fou.getCode());
        xmlCreater.createAttribute(column, "column_remark", "");
    }

    public static void readModelFromDatabase(DatabaseWrapper db, String xmlName) {
        try {
            String userName = db.getName();
            createXml(xmlName, userName);
            List<TableMeta> tableMetas = MetaOperator.getTablesWithColumns(db);
            for (TableMeta tableMeta : tableMetas) {
                String tableName = tableMeta.getTableName();
                String remarks = tableMeta.getRemarks();
                addTable(tableName, remarks, tableMeta);
            }
            xmlCreater.buildXmlFile();
            log.info("写xml信息完成");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("获取表信息失败");
        }
    }

    public static void readModelFromDatabase(DatabaseWrapper db, String xmlName, String tableNameList) {
        try {
            String userName = db.getName();
            File file = FileUtils.getFile(xmlName);
            if (!file.exists()) {
                createXml(xmlName, userName);
            } else {
                openXml(xmlName);
            }
            List<String> names = StringUtil.split(tableNameList, "|");
            for (String name : names) {
                xmlCreater.removeElement(name);
                List<TableMeta> tableMetas = MetaOperator.getTablesWithColumns(db, "%" + name + "%");
                for (TableMeta tableMeta : tableMetas) {
                    String tableName = tableMeta.getTableName();
                    String remarks = tableMeta.getRemarks();
                    addTable(tableName, remarks, tableMeta);
                }
            }
            xmlCreater.buildXmlFile();
            log.info("写xml信息完成" + xmlName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("获取表信息失败");
        }
    }

    public static String getColType(int dataType, String typeName, int precision, int scale, int columnDisplaySize) {
        typeName = StringUtil.replace(typeName, "UNSIGNED", "");
        if (precision != 0) {
            int ic = typeName.indexOf("(");
            if (ic != -1) {
                typeName = typeName.substring(0, ic);
            }
        }
        String column_type;
        if (Types.INTEGER == dataType || Types.TINYINT == dataType || Types.SMALLINT == dataType || Types.BIGINT == dataType || Types.CLOB == dataType || Types.BLOB == dataType || Types.NCLOB == dataType || Types.DATE == dataType || Types.TIME == dataType || Types.TIMESTAMP == dataType || Types.TIME_WITH_TIMEZONE == dataType || Types.TIMESTAMP_WITH_TIMEZONE == dataType || Types.BINARY == dataType || Types.BOOLEAN == dataType) {
            column_type = typeName;
        } else if (Types.NUMERIC == dataType || Types.FLOAT == dataType || Types.DOUBLE == dataType || Types.DECIMAL == dataType) {
            if (0 > precision - Math.abs(scale) || scale > precision || precision == 0) {
                precision = 38;
                scale = 12;
            }
            column_type = typeName + "(" + precision + "," + scale + ")";
        } else if (Types.LONGVARCHAR == dataType) {
            column_type = "TEXT";
        } else {
            if ("string".equalsIgnoreCase(typeName)) {
                return typeName;
            }
            if ("char".equalsIgnoreCase(typeName) && precision > 255) {
                typeName = "varchar";
            }
            if (precision <= 0) {
                column_type = typeName + "(" + columnDisplaySize + ")";
            } else {
                column_type = typeName + "(" + precision + ")";
            }
        }
        return column_type;
    }

    public static List<Map<String, String>> getSqlColumnMeta(String sql, DatabaseWrapper db) {
        Map<String, ColumnMeta> sqlColumnMeta = MetaOperator.getSqlColumnMeta(db, sql);
        List<Map<String, String>> columnList = new ArrayList<>();
        for (String key : sqlColumnMeta.keySet()) {
            ColumnMeta metaData = sqlColumnMeta.get(key);
            Map<String, String> hashMap = new HashMap<>();
            hashMap.put("column_name", key);
            hashMap.put("type", getColType(metaData.getTypeOfSQL(), metaData.getName(), metaData.getLength(), metaData.getScale(), 0));
            columnList.add(hashMap);
        }
        return columnList;
    }
}
