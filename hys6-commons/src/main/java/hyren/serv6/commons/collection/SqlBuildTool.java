package hyren.serv6.commons.collection;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@DocClass(desc = "", author = "HLL", createdate = "2021-6-2 15:58:51")
@Slf4j
public class SqlBuildTool {

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ResultSet getIndexInfo(DatabaseWrapper db, String tableName) {
        log.info("根据表名获取表的额外索引信息: " + tableName);
        ResultSet indexInfo = null;
        try {
            DatabaseMetaData metaData = db.getConnection().getMetaData();
            if (db.getDbtype() == Dbtype.ORACLE) {
                indexInfo = db.queryGetResultSet("select t.*,i.UNIQUENESS from user_ind_columns t,user_indexes i" + " where t.index_name = i.index_name and t.table_name=?", tableName.toUpperCase());
            } else if (db.getDbtype() == Dbtype.MYSQL) {
                indexInfo = db.queryGetResultSet("SELECT * FROM information_schema.statistics WHERE table_name = ?", tableName.toLowerCase());
            } else if (db.getDbtype() == Dbtype.POSTGRESQL) {
                indexInfo = metaData.getIndexInfo(null, null, tableName, false, false);
            } else if (db.getDbtype() == Dbtype.DB2V1 || db.getDbtype() == Dbtype.DB2V2) {
                indexInfo = db.queryGetResultSet("select sysindexes. * from sysibm.sysindexes as sysindexes" + " where tbname = upper(?)", tableName.toUpperCase());
            } else {
                log.warn("获取表索引信息,不支持的数据库类型! " + db.getDbtype());
            }
            log.info("获取表的额外索引信息完成! " + tableName);
        } catch (SQLException e) {
            log.error("获取数据库连接的MetaData失败! " + e);
        }
        return indexInfo;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static void createOtherIndex(DatabaseWrapper db, ResultSet indexInfo, String tableName) {
        log.info("根据获取到的索引信息,重建额外创建的索引: " + tableName);
        Map<String, List<Map<String, Object>>> indexMetaMap = handleIndexMeta(db, indexInfo);
        if (!indexMetaMap.isEmpty()) {
            for (Map.Entry<String, List<Map<String, Object>>> entry : indexMetaMap.entrySet()) {
                String index_name = entry.getKey();
                List<Map<String, Object>> indexColumnMaps = entry.getValue();
                try {
                    StringBuilder indexSql = buildCreateIndexSql(db, indexColumnMaps, index_name, db.getDbtype(), tableName);
                    db.ExecDDL(indexSql.toString());
                } catch (Exception e) {
                    log.error("创建索引异常！tableName: " + tableName + ", index_name: " + index_name);
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    public static void dropTableIndex(DatabaseWrapper db, String tableName) {
        List<String> index_name_s = new ArrayList<>();
        ResultSet indexInfo = getIndexInfo(db, tableName);
        if (null != indexInfo) {
            try {
                while (indexInfo.next()) {
                    index_name_s.add(indexInfo.getString("INDEX_NAME"));
                }
                index_name_s = index_name_s.stream().distinct().collect(Collectors.toList());
                if (db.getDbtype() == Dbtype.POSTGRESQL || db.getDbtype() == Dbtype.ORACLE) {
                    index_name_s.forEach(name -> {
                        if (name.toLowerCase().contains("_hyren_pk")) {
                            db.ExecDDL("ALTER TABLE " + tableName + " DROP CONSTRAINT " + name);
                        } else {
                            db.ExecDDL("DROP INDEX " + name);
                        }
                    });
                } else if (db.getDbtype() == Dbtype.MYSQL) {
                    index_name_s.forEach(name -> db.ExecDDL("DROP INDEX " + name + " ON " + tableName));
                } else {
                    log.error("删除表索引信息,不支持的数据库类型! " + db.getDbtype());
                }
            } catch (Exception e) {
                log.error("删除索引异常！tableName: " + tableName);
                log.error(e.getMessage(), e);
            }
        }
    }

    private static Map<String, List<Map<String, Object>>> handleIndexMeta(DatabaseWrapper db, ResultSet indexInfo) {
        Map<String, List<Map<String, Object>>> indexMetaMap = new HashMap<>();
        try {
            if (db.getDbtype() == Dbtype.POSTGRESQL) {
                while (indexInfo.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("TABLE_SCHEM", indexInfo.getObject("TABLE_SCHEM"));
                    map.put("TABLE_NAME", indexInfo.getObject("TABLE_NAME"));
                    map.put("NON_UNIQUE", indexInfo.getObject("NON_UNIQUE"));
                    String index_name = indexInfo.getString("INDEX_NAME");
                    map.put("INDEX_NAME", index_name);
                    map.put("TYPE", indexInfo.getObject("TYPE"));
                    map.put("ORDINAL_POSITION", indexInfo.getObject("ORDINAL_POSITION"));
                    map.put("COLUMN_NAME", indexInfo.getObject("COLUMN_NAME"));
                    map.put("ASC_OR_DESC", indexInfo.getObject("ASC_OR_DESC"));
                    map.put("PAGES", indexInfo.getObject("PAGES"));
                    map.put("FILTER_CONDITION", indexInfo.getObject("FILTER_CONDITION"));
                    if (StringUtil.isNotBlank(index_name) && !index_name.toLowerCase().contains("_hyren")) {
                        if (indexMetaMap.containsKey(index_name)) {
                            indexMetaMap.get(index_name).add(map);
                        } else {
                            List<Map<String, Object>> indexInfos = new ArrayList<>();
                            indexInfos.add(map);
                            indexMetaMap.put(index_name, indexInfos);
                        }
                    }
                }
            } else if (db.getDbtype() == Dbtype.ORACLE) {
                while (indexInfo.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String index_name = indexInfo.getString("INDEX_NAME");
                    map.put("INDEX_NAME", index_name);
                    map.put("TABLE_NAME", indexInfo.getObject("TABLE_NAME"));
                    map.put("COLUMN_NAME", indexInfo.getObject("COLUMN_NAME"));
                    map.put("COLUMN_POSITION", indexInfo.getObject("COLUMN_POSITION"));
                    map.put("COLUMN_LENGTH", indexInfo.getObject("COLUMN_LENGTH"));
                    map.put("CHAR_LENGTH", indexInfo.getObject("CHAR_LENGTH"));
                    map.put("DESCEND", indexInfo.getObject("DESCEND"));
                    map.put("UNIQUENESS", indexInfo.getObject("UNIQUENESS"));
                    if (StringUtil.isNotBlank(index_name) && !index_name.toLowerCase().contains("_hyren")) {
                        if (indexMetaMap.containsKey(index_name)) {
                            indexMetaMap.get(index_name).add(map);
                        } else {
                            List<Map<String, Object>> indexInfos = new ArrayList<>();
                            indexInfos.add(map);
                            indexMetaMap.put(index_name, indexInfos);
                        }
                    }
                }
            } else if (db.getDbtype() == Dbtype.MYSQL) {
                while (indexInfo.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String index_name = indexInfo.getString("INDEX_NAME");
                    map.put("INDEX_NAME", index_name);
                    map.put("TABLE_SCHEMA", indexInfo.getObject("TABLE_SCHEMA"));
                    map.put("TABLE_NAME", indexInfo.getObject("TABLE_NAME"));
                    map.put("NON_UNIQUE", indexInfo.getObject("NON_UNIQUE"));
                    map.put("INDEX_SCHEMA", indexInfo.getObject("INDEX_SCHEMA"));
                    map.put("SEQ_IN_INDEX", indexInfo.getObject("SEQ_IN_INDEX"));
                    map.put("COLUMN_NAME", indexInfo.getObject("COLUMN_NAME"));
                    map.put("COLLATION", indexInfo.getObject("COLLATION"));
                    map.put("CARDINALITY", indexInfo.getObject("CARDINALITY"));
                    map.put("SUB_PART", indexInfo.getObject("SUB_PART"));
                    map.put("PACKED", indexInfo.getObject("PACKED"));
                    map.put("NULLABLE", indexInfo.getObject("NULLABLE"));
                    map.put("INDEX_TYPE", indexInfo.getObject("INDEX_TYPE"));
                    if (StringUtil.isNotBlank(index_name) && !index_name.toLowerCase().contains("_hyren")) {
                        if (indexMetaMap.containsKey(index_name)) {
                            indexMetaMap.get(index_name).add(map);
                        } else {
                            List<Map<String, Object>> indexInfos = new ArrayList<>();
                            indexInfos.add(map);
                            indexMetaMap.put(index_name, indexInfos);
                        }
                    }
                }
            } else if (db.getDbtype() == Dbtype.DB2V1 || db.getDbtype() == Dbtype.DB2V2) {
                while (indexInfo.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String index_name = indexInfo.getString("NAME");
                    map.put("NAME", index_name);
                    map.put("CREATOR", indexInfo.getObject("CREATOR"));
                    map.put("TBNAME", indexInfo.getObject("TBNAME"));
                    map.put("TBCREATOR", indexInfo.getObject("TBCREATOR"));
                    map.put("COLNAMES", indexInfo.getObject("COLNAMES"));
                    map.put("UNIQUERULE", indexInfo.getObject("UNIQUERULE"));
                    map.put("COLCOUNT", indexInfo.getObject("COLCOUNT"));
                    map.put("DEFINER", indexInfo.getObject("DEFINER"));
                    map.put("MADE_UNIQUE", indexInfo.getObject("MADE_UNIQUE"));
                    map.put("INDEXTYPE", indexInfo.getObject("INDEXTYPE"));
                    map.put("REVERSE_SCANS", indexInfo.getObject("REVERSE_SCANS"));
                    if (StringUtil.isNotBlank(index_name) && !index_name.toLowerCase().contains("_hyren")) {
                        if (indexMetaMap.containsKey(index_name)) {
                            indexMetaMap.get(index_name).add(map);
                        } else {
                            List<Map<String, Object>> indexInfos = new ArrayList<>();
                            indexInfos.add(map);
                            indexMetaMap.put(index_name, indexInfos);
                        }
                    }
                }
            } else {
                log.error("创建额外索引不支持的数据库类型: " + db.getDbtype());
            }
        } catch (SQLException e) {
            log.error("处理数据表索引的meta信息失败: " + e.getMessage());
        }
        return indexMetaMap;
    }

    private static StringBuilder buildCreateIndexSql(DatabaseWrapper db, List<Map<String, Object>> indexColumnMaps, String index_name, Dbtype dbtype, String tableName) {
        StringBuilder sb = new StringBuilder();
        if (dbtype == Dbtype.POSTGRESQL) {
            sb.append("CREATE");
            boolean non_unique = (boolean) indexColumnMaps.get(0).get("NON_UNIQUE");
            if (!non_unique) {
                sb.append(" UNIQUE");
            }
            sb.append(" INDEX ").append(index_name).append(" on ").append(tableName).append(Constant.LXKH);
            indexColumnMaps.forEach(indexColumnMap -> {
                sb.append(indexColumnMap.get("COLUMN_NAME"));
                String asc_or_desc = indexColumnMap.get("ASC_OR_DESC").toString();
                if ("D".equalsIgnoreCase(asc_or_desc)) {
                    sb.append(" DESC");
                }
                sb.append(",");
            });
            sb.delete(sb.length() - 1, sb.length());
            sb.append(Constant.RXKH);
        } else if (dbtype == Dbtype.ORACLE) {
            sb.append("CREATE");
            String uniqueness = (String) indexColumnMaps.get(0).get("UNIQUENESS");
            if ("UNIQUE".equalsIgnoreCase(uniqueness)) {
                sb.append(" UNIQUE");
            }
            sb.append(" INDEX ").append(index_name).append(" on ").append(tableName).append(Constant.LXKH);
            indexColumnMaps.forEach(indexColumnMap -> {
                String column_name = indexColumnMap.get("COLUMN_NAME").toString();
                if (!column_name.contains("SYS_NC")) {
                    sb.append(column_name);
                    String asc_or_desc = indexColumnMap.get("DESCEND").toString();
                    if ("DESC".equalsIgnoreCase(asc_or_desc)) {
                        sb.append(" DESC");
                    }
                    sb.append(",");
                }
            });
            sb.delete(sb.length() - 1, sb.length());
            sb.append(Constant.RXKH);
            sb.append(" ONLINE");
        } else if (dbtype == Dbtype.MYSQL) {
            sb.append("CREATE");
            IsFlag is_non_unique = IsFlag.ofEnumByCode(indexColumnMaps.get(0).get("NON_UNIQUE").toString());
            if (is_non_unique == IsFlag.Fou) {
                sb.append(" UNIQUE");
            }
            if (is_non_unique == IsFlag.Shi && "FULLTEXT".equalsIgnoreCase((String) indexColumnMaps.get(0).get("INDEX_TYPE"))) {
                sb.append(" FULLTEXT");
            }
            sb.append(" INDEX ").append(index_name).append(" on ").append(tableName).append(Constant.LXKH);
            indexColumnMaps.forEach(indexColumnMap -> {
                String column_name = indexColumnMap.get("COLUMN_NAME").toString();
                sb.append(column_name).append(",");
            });
            sb.delete(sb.length() - 1, sb.length());
            sb.append(Constant.RXKH);
        } else if (dbtype == Dbtype.DB2V1 || dbtype == Dbtype.DB2V2) {
            String uniquerule = indexColumnMaps.get(0).get("UNIQUERULE").toString();
            if ("P".equalsIgnoreCase(uniquerule)) {
                sb.append("ALTER TABLE ").append(tableName).append(" ADD CONSTRAINT ").append(index_name).append(" PRIMARY KEY").append(Constant.LXKH);
                indexColumnMaps.forEach(indexColumnMap -> {
                    String colnames = indexColumnMap.get("COLNAMES").toString();
                    colnames = colnames.replace("+", ",").replace("-", ",");
                    colnames = colnames.substring(1);
                    sb.append(colnames).append(",");
                    List<String> column_list = StringUtil.split(colnames, ",");
                    for (String column : column_list) {
                        db.ExecDDL("ALTER TABLE " + tableName + " ALTER " + column + " SET NOT NULL");
                        db.ExecDDL("call sysproc.admin_cmd('reorg table " + tableName + "')");
                    }
                });
            } else {
                sb.append("CREATE");
                if ("U".equalsIgnoreCase(uniquerule)) {
                    sb.append(" UNIQUE INDEX ");
                } else if ("D".equalsIgnoreCase(uniquerule)) {
                    sb.append(" INDEX ");
                } else {
                    log.error(dbtype + " UNIQUERULE 类型不合法! " + uniquerule);
                }
                sb.append(index_name).append(" on ").append(tableName).append(Constant.LXKH);
                indexColumnMaps.forEach(indexColumnMap -> {
                    String column_name = indexColumnMap.get("COLNAMES").toString();
                    column_name = column_name.replace("+", ",").replace("-", ",");
                    column_name = column_name.substring(1);
                    sb.append(column_name).append(",");
                });
            }
            sb.delete(sb.length() - 1, sb.length());
            sb.append(Constant.RXKH);
        } else {
            sb.append("CREATE INDEX ").append(index_name).append(" ON ").append(tableName).append(Constant.LXKH);
            for (Object column : indexColumnMaps) {
                sb.append(column).append(",");
            }
            sb.delete(sb.length() - 1, sb.length());
            sb.append(Constant.RXKH);
        }
        return sb;
    }
}
