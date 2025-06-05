package hyren.serv6.base.utils;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType;
import com.alibaba.druid.sql.dialect.db2.visitor.DB2SchemaStatVisitor;
import com.alibaba.druid.sql.dialect.h2.visitor.H2SchemaStatVisitor;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.mysql.ast.clause.MySqlIterateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.odps.visitor.OdpsSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.oracle.ast.expr.OracleSysdateExpr;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.*;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.sql.dialect.oracle.visitor.OracleSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.phoenix.visitor.PhoenixSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.postgresql.parser.PGSQLStatementParser;
import com.alibaba.druid.sql.dialect.postgresql.visitor.PGSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.sqlserver.visitor.SQLServerSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.stat.TableStat.Name;
import com.alibaba.druid.util.JdbcConstants;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.serv6.base.codes.TableStorage;
import hyren.serv6.base.entity.DmJobTableInfo;
import hyren.serv6.base.entity.DmModuleTable;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import java.util.*;

@Slf4j
public class DruidParseQuerySql {

    public static String sourcecolumn = "sourcecolumn";

    public static String sourcetable = "sourcetable";

    @Getter
    public List<SQLSelectItem> selectList = null;

    public SQLExpr leftWhere = null;

    public SQLExpr rightWhere = null;

    private OracleSelectQueryBlock left = null;

    private final List<HashMap<String, Object>> listmap = new ArrayList<>();

    private final List<HashMap<String, Object>> columnlist = new ArrayList<>();

    private final HashMap<String, Object> hashmap = new HashMap<>();

    private String mainSql = "";

    @Getter
    public List<SQLExpr> allWherelist = new ArrayList<>();

    public DruidParseQuerySql(String sql) {
        OracleStatementParser sqlStatementParser = new OracleStatementParser(sql);
        SQLSelectStatement parseSelect = (SQLSelectStatement) sqlStatementParser.parseSelect();
        SQLSelect select = parseSelect.getSelect();
        SQLSelectQuery query = select.getQuery();
        if (query instanceof SQLUnionQuery) {
            SQLUnionQuery unionQuery = (SQLUnionQuery) query;
            this.left = (OracleSelectQueryBlock) unionQuery.getFirstQueryBlock();
        } else {
            this.left = (OracleSelectQueryBlock) query;
        }
        getAllWhere(query);
        this.selectList = this.left.getSelectList();
        this.leftWhere = this.left.getWhere();
    }

    public DruidParseQuerySql() {
    }

    private void handleFromInWhere(SQLTableSource sqlTableSource, SQLSelectQueryBlock sqlSelectQueryBlock) {
        if (sqlTableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) sqlTableSource;
            SQLExpr condition = sqlJoinTableSource.getCondition();
            allWherelist.add(condition);
            SQLTableSource left = sqlJoinTableSource.getLeft();
            SQLTableSource right = sqlJoinTableSource.getRight();
            handleFromInWhere(left, sqlSelectQueryBlock);
            handleFromInWhere(right, null);
        } else if (sqlTableSource instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) sqlTableSource;
            SQLSelect sqlSelect = sqlSubqueryTableSource.getSelect();
            SQLSelectQuery sqlSelectQuery = sqlSelect.getQuery();
            getAllWhere(sqlSelectQuery);
            if (sqlSelectQueryBlock != null) {
                SQLExpr where = sqlSelectQueryBlock.getWhere();
                allWherelist.add(where);
            }
        } else if (sqlTableSource instanceof SQLExprTableSource) {
            if (sqlSelectQueryBlock != null) {
                SQLExpr where = sqlSelectQueryBlock.getWhere();
                putWhereIntoList(where);
            }
        }
    }

    private void putWhereIntoList(SQLExpr where) {
        if (where instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) where;
            SQLBinaryOperator operator = sqlBinaryOpExpr.getOperator();
            if (operator == SQLBinaryOperator.BooleanAnd || operator == SQLBinaryOperator.BooleanXor || operator == SQLBinaryOperator.BooleanOr) {
                SQLExpr left = sqlBinaryOpExpr.getLeft();
                putWhereIntoList(left);
                SQLExpr right = sqlBinaryOpExpr.getRight();
                putWhereIntoList(right);
            } else {
                allWherelist.add(where);
            }
        } else {
            allWherelist.add(where);
        }
    }

    private void getAllWhere(SQLSelectQuery query) {
        if (query instanceof SQLUnionQuery) {
            SQLSelectQuery left = ((SQLUnionQuery) query).getLeft();
            getAllWhere(left);
            SQLSelectQuery right = ((SQLUnionQuery) query).getRight();
            getAllWhere(right);
        } else if (query instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) query;
            SQLTableSource sqlTableSource = sqlSelectQueryBlock.getFrom();
            handleFromInWhere(sqlTableSource, sqlSelectQueryBlock);
        }
    }

    public List<String> parseSelectOriginalField() {
        List<String> originalColumnSet = new ArrayList<>();
        selectList.forEach(val -> {
            SQLExpr expr = val.getExpr();
            if (expr instanceof SQLPropertyExpr) {
                originalColumnSet.add(expr.toString());
            } else if (expr instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
                originalColumnSet.add(identifierExpr.getName());
            } else {
                originalColumnSet.add(val.getAlias());
            }
        });
        return originalColumnSet;
    }

    public List<String> parseSelectAliasField() {
        List<String> aliasColumnSet = new ArrayList<>();
        selectList.forEach(val -> {
            if (StringUtil.isNotBlank(val.getAlias())) {
                aliasColumnSet.add(val.getAlias());
                return;
            }
            SQLExpr expr = val.getExpr();
            if (expr instanceof SQLPropertyExpr) {
                aliasColumnSet.add(((SQLPropertyExpr) expr).getName());
            } else if (expr instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
                aliasColumnSet.add(identifierExpr.getName());
            } else {
                aliasColumnSet.add(val.getAlias());
            }
        });
        return aliasColumnSet;
    }

    public void setSelectList(List<SQLSelectItem> selectList) {
        this.selectList = selectList;
    }

    public static Set<Name> parseSqlTable(String sql) {
        try {
            SQLStatementParser parse = new OracleStatementParser(sql);
            SQLStatement parseStatement = parse.parseStatement();
            OracleSchemaStatVisitor visitor = new OracleSchemaStatVisitor();
            parseStatement.accept(visitor);
            Map<TableStat.Name, TableStat> tables = visitor.getTables();
            return tables.keySet();
        } catch (Exception e) {
            SQLStatementParser parse = new PGSQLStatementParser(sql);
            SQLStatement parseStatement = parse.parseStatement();
            PGSchemaStatVisitor visitor = new PGSchemaStatVisitor();
            parseStatement.accept(visitor);
            Map<TableStat.Name, TableStat> tables = visitor.getTables();
            return tables.keySet();
        }
    }

    public static List<String> parseSqlTableToList(String sql) {
        List<String> tableList = new ArrayList<>();
        Set<Name> parseSqlTable = parseSqlTable(sql);
        for (Name name : parseSqlTable) {
            tableList.add(name.toString());
        }
        return tableList;
    }

    public HashMap<String, Object> getBloodRelationMap(String sql) {
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        this.listmap.clear();
        this.columnlist.clear();
        this.hashmap.clear();
        DbType dbType = JdbcConstants.ORACLE;
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        for (SQLStatement stmt : stmtList) {
            SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) stmt;
            SQLSelect sqlSelect = sqlSelectStatement.getSelect();
            SQLSelectQuery sqlSelectQuery = sqlSelect.getQuery();
            getBloodRelation(sqlSelectQuery);
        }
        return hashmap;
    }

    private void getBloodRelation(SQLSelectQuery sqlSelectQuery) {
        if (sqlSelectQuery instanceof SQLUnionQuery) {
            SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) sqlSelectQuery;
            getBloodRelation(sqlUnionQuery.getLeft());
            getBloodRelation(sqlUnionQuery.getRight());
        } else if (sqlSelectQuery instanceof OracleSelectQueryBlock) {
            OracleSelectQueryBlock oracleSelectQueryBlock = (OracleSelectQueryBlock) sqlSelectQuery;
            this.mainSql = oracleSelectQueryBlock.toString();
            handleFrom(oracleSelectQueryBlock.getFrom());
        } else {
            throw new SystemBusinessException("未知的sqlSelectQuery：" + sqlSelectQuery.toString() + " class:" + sqlSelectQuery.getClass());
        }
    }

    private void handleFrom(SQLTableSource sqlTableSource) {
        if (sqlTableSource instanceof OracleSelectJoin) {
            OracleSelectJoin oracleSelectJoin = (OracleSelectJoin) sqlTableSource;
            handleFrom(oracleSelectJoin.getLeft());
            handleFrom(oracleSelectJoin.getRight());
        } else if (sqlTableSource instanceof OracleSelectSubqueryTableSource) {
            OracleSelectSubqueryTableSource oracleSelectSubqueryTableSource = (OracleSelectSubqueryTableSource) sqlTableSource;
            SQLSelect sqlSelect = oracleSelectSubqueryTableSource.getSelect();
            SQLSelectQuery sqlSelectQuery = sqlSelect.getQuery();
            handleFrom2(sqlSelectQuery);
        } else if (sqlTableSource instanceof OracleSelectTableReference) {
            OracleSelectTableReference oracleSelectTableReference = (OracleSelectTableReference) sqlTableSource;
            SQLObject sqlObject = oracleSelectTableReference.getParent();
            while (!(sqlObject instanceof OracleSelectQueryBlock)) {
                sqlObject = sqlObject.getParent();
            }
            listmap.clear();
            String upperealias = "";
            while (!trim(sqlObject.toString()).equalsIgnoreCase(trim(mainSql))) {
                OracleSelectQueryBlock oracleSelectQueryBlock = (OracleSelectQueryBlock) sqlObject;
                startHandleFromColumn(oracleSelectQueryBlock, sqlTableSource, upperealias);
                sqlObject = sqlObject.getParent();
                if (sqlObject.getParent() instanceof OracleSelectSubqueryTableSource) {
                    OracleSelectSubqueryTableSource oracleSelectSubqueryTableSource = (OracleSelectSubqueryTableSource) sqlObject.getParent();
                    upperealias = oracleSelectSubqueryTableSource.getAlias();
                }
                while (!(sqlObject instanceof OracleSelectQueryBlock)) {
                    sqlObject = sqlObject.getParent();
                }
            }
            OracleSelectQueryBlock oracleSelectQueryBlock = (OracleSelectQueryBlock) sqlObject;
            boolean isOracleSelectTableReference = oracleSelectQueryBlock.getFrom() instanceof OracleSelectTableReference;
            handleGetColumn(oracleSelectQueryBlock);
            if (listmap.isEmpty()) {
                for (HashMap<String, Object> stringObjectHashMap : columnlist) {
                    SQLExpr sqlexpr = (SQLExpr) stringObjectHashMap.get("column");
                    String alias = stringObjectHashMap.get("alias").toString();
                    ArrayList<HashMap<String, Object>> templist = getTempList(alias);
                    if (sqlexpr instanceof SQLIdentifierExpr) {
                        if (isOracleSelectTableReference) {
                            putResult(templist, alias, sqlexpr.toString(), sqlTableSource.toString());
                        } else {
                            throw new SystemBusinessException("请填写标准sql 字段" + sqlexpr + "未知来源");
                        }
                    } else if (sqlexpr instanceof SQLPropertyExpr) {
                        SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlexpr;
                        SQLExpr owner = sqlPropertyExpr.getOwner();
                        String alias1 = oracleSelectTableReference.getAlias();
                        SQLExpr expr = oracleSelectTableReference.getExpr();
                        if (owner.toString().equals(alias1) || owner.equals(expr)) {
                            putResult(templist, alias, sqlPropertyExpr.getName(), sqlTableSource.toString());
                        }
                    }
                }
            } else {
                for (HashMap<String, Object> stringObjectHashMap : listmap) {
                    Object uppercolumnObj = stringObjectHashMap.get("uppercolumn");
                    List<String> uppercolumnlist = new ArrayList<>();
                    if (uppercolumnObj instanceof ArrayList<?>) {
                        for (Object uppercolumn : (List<?>) uppercolumnObj) {
                            uppercolumnlist.add((String) uppercolumn);
                        }
                    }
                    for (String uppercolumn : uppercolumnlist) {
                        for (HashMap<String, Object> objectHashMap : columnlist) {
                            SQLExpr sqlexpr = (SQLExpr) objectHashMap.get("column");
                            String alias = objectHashMap.get("alias").toString();
                            ArrayList<HashMap<String, Object>> templist = getTempList(alias);
                            if (sqlexpr instanceof SQLIdentifierExpr) {
                                if (uppercolumn.equalsIgnoreCase(sqlexpr.toString())) {
                                    putResult(templist, alias, stringObjectHashMap.get("columnname") == null ? null : stringObjectHashMap.get("columnname").toString(), stringObjectHashMap.get("table").toString());
                                }
                            } else if (sqlexpr instanceof SQLPropertyExpr) {
                                SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlexpr;
                                if (uppercolumn.equalsIgnoreCase(sqlPropertyExpr.getName()) && sqlPropertyExpr.getOwner().toString().equalsIgnoreCase(upperealias)) {
                                    putResult(templist, alias, stringObjectHashMap.get("columnname") == null ? null : stringObjectHashMap.get("columnname").toString(), stringObjectHashMap.get("table").toString());
                                }
                            }
                        }
                    }
                }
            }
        } else {
            String message;
            if (sqlTableSource == null) {
                message = "未知的From来源";
            } else {
                message = "未知的From来源：" + sqlTableSource + " class:" + sqlTableSource.getClass();
            }
            throw new SystemBusinessException(message);
        }
    }

    private void putResult(ArrayList<HashMap<String, Object>> templist, String alias, String columnname, String table) {
        if (columnname != null) {
            HashMap<String, Object> temphashmap = new HashMap<>();
            if (columnname.contains(" ")) {
                columnname = StringUtil.split(columnname.toLowerCase(), " ").get(0);
            }
            if (table.contains(" ")) {
                table = StringUtil.split(table.toLowerCase(), " ").get(0);
            }
            temphashmap.put(sourcecolumn, columnname);
            temphashmap.put(sourcetable, table);
            templist.add(temphashmap);
        }
    }

    @SuppressWarnings("unchecked")
    private ArrayList<HashMap<String, Object>> getTempList(String alias) {
        ArrayList<HashMap<String, Object>> templist = new ArrayList<>();
        if (hashmap.get(alias) != null) {
            templist = (ArrayList<HashMap<String, Object>>) hashmap.get(alias);
        } else {
            hashmap.put(alias, templist);
        }
        return templist;
    }

    private void handleFrom2(SQLSelectQuery sqlSelectQuery) {
        if (sqlSelectQuery instanceof SQLUnionQuery) {
            SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) sqlSelectQuery;
            handleFrom2(sqlUnionQuery.getLeft());
            handleFrom2(sqlUnionQuery.getRight());
        } else if (sqlSelectQuery instanceof OracleSelectQueryBlock) {
            OracleSelectQueryBlock oracleSelectQueryBlock = (OracleSelectQueryBlock) sqlSelectQuery;
            handleFrom(oracleSelectQueryBlock.getFrom());
        } else {
            String message;
            if (sqlSelectQuery == null) {
                message = "未知的SelectQuery";
            } else {
                message = "未知的SelectQuery来源：" + sqlSelectQuery + " class:" + sqlSelectQuery.getClass();
            }
            throw new SystemBusinessException(message);
        }
    }

    private void startHandleFromColumn(OracleSelectQueryBlock oracleSelectQueryBlock, SQLTableSource sqlTableSource, String upperealias) {
        SQLTableSource from = oracleSelectQueryBlock.getFrom();
        Boolean isOracleSelectTableReference = oracleSelectQueryBlock.getFrom() instanceof OracleSelectTableReference;
        if (listmap.isEmpty()) {
            if (sqlTableSource instanceof OracleSelectTableReference) {
                OracleSelectTableReference oracleSelectTableReference = (OracleSelectTableReference) sqlTableSource;
                List<SQLSelectItem> selectList = oracleSelectQueryBlock.getSelectList();
                for (SQLSelectItem sqlSelectItem : selectList) {
                    handleColumn(sqlSelectItem, oracleSelectTableReference, isOracleSelectTableReference);
                }
            }
        } else {
            for (int i = 0; i < listmap.size(); i++) {
                HashMap<String, Object> stringObjectHashMap = listmap.get(i);
                List<String> uppercolumnlist = new ArrayList<>();
                Object uppercolumnObj = stringObjectHashMap.get("uppercolumn");
                if (uppercolumnObj instanceof ArrayList<?>) {
                    for (Object uppercolumn : (List<?>) uppercolumnObj) {
                        uppercolumnlist.add((String) uppercolumn);
                    }
                }
                List<String> newuppercolumnlist = new ArrayList<>();
                for (String uppercolumn : uppercolumnlist) {
                    handleGetColumn(oracleSelectQueryBlock);
                    boolean flag = true;
                    for (HashMap<String, Object> objectHashMap : columnlist) {
                        SQLExpr sqlexpr = (SQLExpr) objectHashMap.get("column");
                        String alias = objectHashMap.get("alias").toString();
                        if (sqlexpr instanceof SQLIdentifierExpr) {
                            if (uppercolumn.equalsIgnoreCase(sqlexpr.toString())) {
                                newuppercolumnlist.add(alias);
                                flag = false;
                            }
                        } else if (sqlexpr instanceof SQLPropertyExpr) {
                            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlexpr;
                            if (uppercolumn.equalsIgnoreCase(sqlPropertyExpr.getName()) && sqlPropertyExpr.getOwner().toString().equalsIgnoreCase(upperealias)) {
                                newuppercolumnlist.add(alias);
                                flag = false;
                            }
                        }
                    }
                    if (flag) {
                        listmap.remove(i);
                    }
                }
                stringObjectHashMap.put("uppercolumn", newuppercolumnlist);
            }
        }
    }

    private String getAlias(SQLSelectItem sqlSelectItem) {
        String alias = sqlSelectItem.getAlias();
        if (StringUtils.isBlank(alias)) {
            if (sqlSelectItem.getExpr() instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlSelectItem.getExpr();
                alias = sqlIdentifierExpr.toString();
            } else if (sqlSelectItem.getExpr() instanceof SQLPropertyExpr) {
                SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlSelectItem.getExpr();
                alias = sqlPropertyExpr.getName();
            } else if (sqlSelectItem.getExpr() instanceof SQLAllColumnExpr) {
                throw new SystemBusinessException("请明确字段，禁止使用 * 代替字段");
            } else {
                throw new SystemBusinessException("请明确字段 " + sqlSelectItem.getExpr() + " 没有别名 ");
            }
        }
        return alias;
    }

    private void handleColumn(SQLSelectItem sqlSelectItem, OracleSelectTableReference oracleSelectTableReference, Boolean isOracleSelectTableReference) {
        String alias = getAlias(sqlSelectItem);
        columnlist.clear();
        getcolumn(sqlSelectItem.getExpr(), sqlSelectItem.getAlias());
        for (HashMap<String, Object> stringObjectHashMap : columnlist) {
            HashMap<String, Object> map = new HashMap<>();
            SQLExpr column = (SQLExpr) stringObjectHashMap.get("column");
            if (column instanceof SQLIdentifierExpr) {
                if (isOracleSelectTableReference) {
                    SQLIdentifierExpr sqlIdentifierExprcolumn = (SQLIdentifierExpr) column;
                    map.put("columnname", sqlIdentifierExprcolumn.getName());
                } else {
                    throw new SystemBusinessException("请填写标准sql 字段" + column + "未知来源");
                }
            } else if (column instanceof SQLPropertyExpr) {
                SQLPropertyExpr sqlPropertyExprcolumn = (SQLPropertyExpr) column;
                String owner = sqlPropertyExprcolumn.getOwner().toString();
                String tablename = StringUtils.isEmpty(oracleSelectTableReference.getAlias()) ? oracleSelectTableReference.getName().toString() : oracleSelectTableReference.getAlias();
                if (owner.trim().equalsIgnoreCase(tablename.trim())) {
                    map.put("columnname", sqlPropertyExprcolumn.getName());
                }
            } else {
                throw new SystemBusinessException("未知的column类型 column:" + column + " 类型：" + column.getClass());
            }
            map.put("table", oracleSelectTableReference.toString());
            List<String> list = new ArrayList<>();
            list.add(alias);
            map.put("uppercolumn", list);
            listmap.add(map);
        }
    }

    public void getcolumn(SQLExpr sqlexpr, String alias) {
        if (null == sqlexpr) {
            return;
        }
        if (sqlexpr instanceof SQLIdentifierExpr) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("column", sqlexpr);
            map.put("alias", alias);
            columnlist.add(map);
        } else if (sqlexpr instanceof SQLPropertyExpr) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("column", sqlexpr);
            map.put("alias", alias);
            columnlist.add(map);
        } else if (sqlexpr instanceof SQLAggregateExpr) {
            SQLAggregateExpr sqlAggregateExpr = (SQLAggregateExpr) sqlexpr;
            List<SQLExpr> arguments = sqlAggregateExpr.getArguments();
            for (SQLExpr sqlExpr : arguments) {
                getcolumn(sqlExpr, alias);
            }
        } else if (sqlexpr instanceof SQLAllColumnExpr) {
            throw new SystemBusinessException("SQLAllColumnExpr 未开发 有待开发");
        } else if (sqlexpr instanceof SQLAllExpr) {
            throw new SystemBusinessException("SQLAllExpr未开发 有待开发");
        } else if (sqlexpr instanceof SQLAnyExpr) {
            throw new SystemBusinessException("SQLAnyExpr 有待开发");
        } else if (sqlexpr instanceof SQLArrayExpr) {
            throw new SystemBusinessException("SQLArrayExpr 有待开发");
        } else if (sqlexpr instanceof SQLBetweenExpr) {
            SQLBetweenExpr sqlBetweenExpr = (SQLBetweenExpr) sqlexpr;
            getcolumn(sqlBetweenExpr.getBeginExpr(), alias);
            getcolumn(sqlBetweenExpr.getTestExpr(), alias);
            getcolumn(sqlBetweenExpr.getEndExpr(), alias);
        } else if (sqlexpr instanceof SQLBinaryExpr) {
            throw new SystemBusinessException("SQLBinaryExpr 有待开发");
        } else if (sqlexpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlexpr;
            getcolumn(sqlBinaryOpExpr.getLeft(), alias);
            getcolumn(sqlBinaryOpExpr.getRight(), alias);
        } else if (sqlexpr instanceof SQLBinaryOpExprGroup) {
            throw new SystemBusinessException("SQLBinaryOpExprGroup 有待开发");
        } else if (sqlexpr instanceof SQLBooleanExpr) {
            log.debug("Ignore, SQLBooleanExpr 有待开发");
        } else if (sqlexpr instanceof SQLCaseExpr) {
            SQLCaseExpr sqlCaseExpr = (SQLCaseExpr) sqlexpr;
            List<SQLCaseExpr.Item> items = sqlCaseExpr.getItems();
            for (SQLCaseExpr.Item item : items) {
                getcolumn(item.getValueExpr(), alias);
            }
            SQLExpr elseExpr = sqlCaseExpr.getElseExpr();
            getcolumn(elseExpr, alias);
            SQLExpr valueExpr = sqlCaseExpr.getValueExpr();
            getcolumn(valueExpr, alias);
        } else if (sqlexpr instanceof SQLCaseStatement) {
            throw new SystemBusinessException("SQLCaseStatement 有待开发");
        } else if (sqlexpr instanceof SQLCastExpr) {
            SQLCastExpr sqlCastExpr = (SQLCastExpr) sqlexpr;
            getcolumn(sqlCastExpr.getExpr(), alias);
        } else if (sqlexpr instanceof SQLCharExpr) {
            log.debug("Ignore, SQLCharExpr 有待开发");
        } else if (sqlexpr instanceof SQLContainsExpr) {
            SQLContainsExpr sqlContainsExpr = (SQLContainsExpr) sqlexpr;
            getcolumn(sqlContainsExpr.getExpr(), alias);
            List<SQLExpr> targetList = sqlContainsExpr.getTargetList();
            for (SQLExpr sqlExpr : targetList) {
                getcolumn(sqlExpr, alias);
            }
        } else if (sqlexpr instanceof SQLCurrentOfCursorExpr) {
            throw new SystemBusinessException("SQLCurrentOfCursorExpr 有待开发");
        } else if (sqlexpr instanceof SQLDateExpr) {
            throw new SystemBusinessException("SQLDateExpr 有待开发");
        } else if (sqlexpr instanceof SQLExistsExpr) {
            throw new SystemBusinessException("SQLExistsExpr 有待开发");
        } else if (sqlexpr instanceof SQLExprUtils) {
            throw new SystemBusinessException("SQLExprUtils 有待开发");
        } else if (sqlexpr instanceof SQLFlashbackExpr) {
            throw new SystemBusinessException("SQLFlashbackExpr 有待开发");
        } else if (sqlexpr instanceof SQLGroupingSetExpr) {
            SQLGroupingSetExpr sqlGroupingSetExpr = (SQLGroupingSetExpr) sqlexpr;
            List<SQLExpr> parameters = sqlGroupingSetExpr.getParameters();
            for (SQLExpr sqlExpr : parameters) {
                getcolumn(sqlexpr, alias);
            }
        } else if (sqlexpr instanceof SQLHexExpr) {
            log.debug("Ignore, SQLHexExpr 有待开发");
        } else if (sqlexpr instanceof SQLInListExpr) {
            SQLInListExpr sqlInListExpr = (SQLInListExpr) sqlexpr;
            getcolumn(sqlInListExpr.getExpr(), alias);
            List<SQLExpr> targetList = sqlInListExpr.getTargetList();
            for (SQLExpr sqlExpr : targetList) {
                getcolumn(sqlExpr, alias);
            }
        } else if (sqlexpr instanceof SQLInSubQueryExpr) {
            SQLInSubQueryExpr sqlInSubQueryExpr = (SQLInSubQueryExpr) sqlexpr;
            getcolumn(sqlInSubQueryExpr.getExpr(), alias);
        } else if (sqlexpr instanceof SQLIntegerExpr) {
            log.debug("Ignore, SQLIntegerExpr 有待开发");
        } else if (sqlexpr instanceof SQLIntervalExpr) {
            log.debug("Ignore, SQLIntervalExpr 有待开发");
        } else if (sqlexpr instanceof SQLListExpr) {
            SQLListExpr sqlListExpr = (SQLListExpr) sqlexpr;
            List<SQLExpr> items = sqlListExpr.getItems();
            for (SQLExpr sqlExpr : items) {
                getcolumn(sqlExpr, alias);
            }
        } else if (sqlexpr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr) sqlexpr;
            List<SQLExpr> parameters = sqlMethodInvokeExpr.getArguments();
            for (SQLExpr sqlExpr : parameters) {
                getcolumn(sqlExpr, alias);
            }
        } else if (sqlexpr instanceof SQLNCharExpr) {
            log.debug("Ignore, SQLNCharExpr 有待开发");
        } else if (sqlexpr instanceof SQLNotExpr) {
            SQLNotExpr sqlNotExpr = (SQLNotExpr) sqlexpr;
            getcolumn(sqlNotExpr.getExpr(), alias);
        } else if (sqlexpr instanceof SQLNullExpr) {
            log.debug("Ignore, SQLNullExpr 有待开发");
        } else if (sqlexpr instanceof SQLNumberExpr) {
            log.debug("Ignore, SQLNumberExpr 有待开发");
        } else if (sqlexpr instanceof SQLQueryExpr) {
            throw new SystemBusinessException("SQLQueryExpr 有待开发");
        } else if (sqlexpr instanceof SQLRealExpr) {
            log.debug("Ignore, SQLRealExpr 有待开发");
        } else if (sqlexpr instanceof SQLSequenceExpr) {
            throw new SystemBusinessException("SQLSequenceExpr 有待开发");
        } else if (sqlexpr instanceof SQLSomeExpr) {
            throw new SystemBusinessException("SQLSomeExpr 有待开发");
        } else if (sqlexpr instanceof SQLTextLiteralExpr) {
            log.debug("Ignore, SQLTextLiteralExpr 有待开发");
        } else if (sqlexpr instanceof SQLTimestampExpr) {
            log.debug("Ignore, SQLTimestampExpr 有待开发");
        } else if (sqlexpr instanceof SQLUnaryExpr) {
            SQLUnaryExpr sqlUnaryExpr = (SQLUnaryExpr) sqlexpr;
            getcolumn(sqlUnaryExpr.getExpr(), alias);
        } else if (sqlexpr instanceof SQLValuesExpr) {
            SQLValuesExpr sqlValuesExpr = (SQLValuesExpr) sqlexpr;
            List<SQLListExpr> values = sqlValuesExpr.getValues();
            for (SQLListExpr sqlListExpr : values) {
                getcolumn(sqlListExpr, alias);
            }
        } else if (sqlexpr instanceof SQLVariantRefExpr) {
            log.debug("Ignore, SQLVariantRefExpr 有待开发");
        } else if (sqlexpr instanceof OracleSysdateExpr) {
            log.debug("Ignore, OracleSysdateExpr 有待开发");
        } else {
            throw new SystemBusinessException("未知的sqlexpr类型 sqlexpr：" + sqlexpr + "class:" + sqlexpr.getClass());
        }
    }

    private String trim(String sql) {
        sql = sql.replace("\r", "");
        sql = sql.replace("\r", "");
        sql = sql.replace("\t", "");
        sql = sql.replace(" ", "");
        sql = sql.trim();
        return sql;
    }

    private void handleGetColumn(OracleSelectQueryBlock oracleSelectQueryBlock) {
        List<SQLSelectItem> selectList = oracleSelectQueryBlock.getSelectList();
        columnlist.clear();
        for (SQLSelectItem sqlSelectItem : selectList) {
            String alias = getAlias(sqlSelectItem);
            getcolumn(sqlSelectItem.getExpr(), alias);
        }
    }

    public String GetNewSql(String sql) {
        DbType dbType = JdbcConstants.ORACLE;
        HashMap<String, String> viewMap = new HashMap<>();
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            for (SQLStatement stmt : stmtList) {
                SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) stmt;
                SQLSelect sqlSelect = sqlSelectStatement.getSelect();
                SQLSelectQuery sqlSelectQuery = sqlSelect.getQuery();
                setFrom(sqlSelectQuery, db, viewMap);
            }
        }
        for (String key : viewMap.keySet()) {
            sql = sql.replaceAll(key, viewMap.get(key));
        }
        return sql;
    }

    private void setFrom(SQLSelectQuery sqlSelectQuery, DatabaseWrapper db, HashMap<String, String> viewMap) {
        if (sqlSelectQuery instanceof SQLUnionQuery) {
            SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) sqlSelectQuery;
            setFrom(sqlUnionQuery.getLeft(), db, viewMap);
            setFrom(sqlUnionQuery.getRight(), db, viewMap);
        } else if (sqlSelectQuery instanceof OracleSelectQueryBlock) {
            OracleSelectQueryBlock oracleSelectQueryBlock = (OracleSelectQueryBlock) sqlSelectQuery;
            handleSetFrom(oracleSelectQueryBlock.getFrom(), db, viewMap);
        } else {
            String message;
            if (sqlSelectQuery == null) {
                message = "SelectQuery 为空";
            } else {
                message = "未知的SelectQuery来源：" + sqlSelectQuery + " class:" + sqlSelectQuery.getClass();
            }
            throw new SystemBusinessException(message);
        }
    }

    private void handleSetFrom(SQLTableSource sqlTableSource, DatabaseWrapper db, HashMap<String, String> viewMap) {
        if (sqlTableSource instanceof OracleSelectJoin) {
            OracleSelectJoin oracleSelectJoin = (OracleSelectJoin) sqlTableSource;
            handleSetFrom(oracleSelectJoin.getLeft(), db, viewMap);
            handleSetFrom(oracleSelectJoin.getRight(), db, viewMap);
        } else if (sqlTableSource instanceof OracleSelectSubqueryTableSource) {
            OracleSelectSubqueryTableSource oracleSelectSubqueryTableSource = (OracleSelectSubqueryTableSource) sqlTableSource;
            SQLSelect sqlSelect = oracleSelectSubqueryTableSource.getSelect();
            SQLSelectQuery sqlSelectQuery = sqlSelect.getQuery();
            setFrom(sqlSelectQuery, db, viewMap);
        } else if (sqlTableSource instanceof OracleSelectTableReference) {
            OracleSelectTableReference oracleSelectTableReference = (OracleSelectTableReference) sqlTableSource;
            String tablename = oracleSelectTableReference.getExpr().toString();
            List<Map<String, Object>> maps = SqlOperator.queryList(db, "select t2.jobtab_execute_sql from " + DmModuleTable.TableName + " t1 left join " + DmJobTableInfo.TableName + " t2 on t1.module_table_id = t2.module_table_id" + " where lower(t1.module_table_en_name) = ? and t1.table_storage = ?", tablename.toLowerCase(), TableStorage.ShuJuShiTu.getCode());
            if (!maps.isEmpty()) {
                String execute_sql = maps.get(0).get("execute_sql").toString();
                viewMap.put(tablename, " ( " + execute_sql + " ) " + tablename);
            }
        } else {
            String message;
            if (sqlTableSource == null) {
                message = "sqlTableSource";
            } else {
                message = "未知的sqlTableSource来源：" + sqlTableSource + " class:" + sqlTableSource.getClass();
            }
            throw new BusinessException(message);
        }
    }

    public static String getInDeUpSqlTableName(String sql) {
        String tablename = "";
        DbType dbType = JdbcConstants.ORACLE;
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        for (SQLStatement stmt : stmtList) {
            if (stmt instanceof OracleUpdateStatement) {
                OracleUpdateStatement oracleUpdateStatement = (OracleUpdateStatement) stmt;
                SQLName tableName = oracleUpdateStatement.getTableName();
                tablename = tableName.toString();
            } else if (stmt instanceof OracleInsertStatement) {
                OracleInsertStatement oracleInsertStatement = (OracleInsertStatement) stmt;
                SQLName tableName = oracleInsertStatement.getTableName();
                tablename = tableName.toString();
            } else if (stmt instanceof OracleDeleteStatement) {
                OracleDeleteStatement oracleDeleteStatement = (OracleDeleteStatement) stmt;
                SQLName tableName = oracleDeleteStatement.getTableName();
                tablename = tableName.toString();
            } else {
                throw new BusinessException("SQL非Delete,Update或者Insert中的一种，请检查");
            }
        }
        return tablename;
    }

    public static String getMysqlSqlTableName(String sql) {
        String tablename = null;
        DbType dbType = JdbcConstants.MYSQL;
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        for (SQLStatement stmt : stmtList) {
            if (stmt instanceof MySqlUpdateStatement) {
                MySqlUpdateStatement oracleUpdateStatement = (MySqlUpdateStatement) stmt;
                SQLName tableName = oracleUpdateStatement.getTableName();
                tablename = tableName.toString();
            } else if (stmt instanceof MySqlIterateStatement) {
                MySqlIterateStatement oracleInsertStatement = (MySqlIterateStatement) stmt;
                tablename = oracleInsertStatement.getLabelName();
            } else if (stmt instanceof MySqlDeleteStatement) {
                MySqlDeleteStatement oracleDeleteStatement = (MySqlDeleteStatement) stmt;
                tablename = oracleDeleteStatement.getTableName().toString();
                SQLTableSource from = ((MySqlDeleteStatement) stmt).getFrom();
                if (from != null) {
                    if (((SQLJoinTableSource) from).getJoinType() == JoinType.INNER_JOIN) {
                        SQLExprTableSource left = (SQLExprTableSource) ((SQLJoinTableSource) from).getLeft();
                        String alias = left.getAlias();
                        if (tablename.equals(alias)) {
                            tablename = left.getExpr().toString();
                        }
                    }
                }
            } else {
                throw new BusinessException("SQL非Delete,Update或者Insert中的一种，请检查");
            }
        }
        return tablename;
    }

    public String getSelectSql() {
        String selectSql = left.toString();
        selectSql = selectSql.substring(selectSql.indexOf("SELECT"), selectSql.indexOf("\nFROM ") + 6);
        return selectSql;
    }

    public Map<String, String> getSelectColumnMap() {
        Map<String, String> selectColumnMap = new HashMap<>();
        for (SQLSelectItem sqlSelectItem : selectList) {
            if (sqlSelectItem.getAlias() == null) {
                String selectColumn = sqlSelectItem.getExpr().toString().toUpperCase();
                if (selectColumn.contains(".")) {
                    selectColumn = StringUtil.split(selectColumn, ".").get(1);
                }
                selectColumnMap.put(selectColumn, sqlSelectItem.getExpr().toString());
            } else {
                selectColumnMap.put(sqlSelectItem.getAlias().toUpperCase(), sqlSelectItem.getExpr().toString());
            }
        }
        return selectColumnMap;
    }

    public static Map<String, Object> analysisTableRelation(String sql, String type) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, type);
        DruidParseQuerySql druidParseQuerySql = new DruidParseQuerySql();
        Map<String, Object> targetTableDataMap = new HashMap<>();
        for (SQLStatement sqlStatement : stmtList) {
            Map<String, Object> bloodRelationMap = null;
            if (sqlStatement instanceof SQLInsertStatement) {
                bloodRelationMap = druidParseQuerySql.getBloodRelationMap(((SQLInsertStatement) sqlStatement).getQuery().toString());
            } else if (sqlStatement instanceof SQLDeleteStatement) {
                log.debug("删除SQL解析未实现! sql:" + sql);
            } else if (sqlStatement instanceof SQLUpdateStatement) {
                log.debug("更新SQL解析未实现! sql:" + sql);
            } else if (sqlStatement instanceof SQLSelectStatement) {
                bloodRelationMap = druidParseQuerySql.getBloodRelationMap(((SQLSelectStatement) sqlStatement).getSelect().getQuery().toString());
            } else if (sqlStatement instanceof SQLCreateTableStatement) {
                SQLSelect select = ((SQLCreateTableStatement) sqlStatement).getSelect();
                if (select != null) {
                    bloodRelationMap = druidParseQuerySql.getBloodRelationMap(select.getQuery().toString());
                }
            }
            targetTableDataMap.put("targetTableField", bloodRelationMap);
            SchemaStatVisitor visitor = getVisitor(sqlStatement, type);
            targetTableDataMap.put("tableName", visitor.getTables().keySet().toArray()[0].toString());
        }
        return targetTableDataMap;
    }

    private static SchemaStatVisitor getVisitor(SQLStatement stmt, String type) {
        SchemaStatVisitor visitor;
        if (type.toLowerCase().equals(DbType.teradata.toString())) {
            visitor = new PGSchemaStatVisitor();
        } else if (type.toLowerCase().equals(DbType.oracle.toString())) {
            visitor = new OracleSchemaStatVisitor();
        } else if (type.toLowerCase().equals(DbType.mysql.toString())) {
            visitor = new MySqlSchemaStatVisitor();
        } else if (type.toLowerCase().equals(DbType.phoenix.toString())) {
            visitor = new PhoenixSchemaStatVisitor();
        } else if (type.toLowerCase().equals(DbType.postgresql.toString())) {
            visitor = new PGSchemaStatVisitor();
        } else if (type.toLowerCase().equals(DbType.sqlserver.toString())) {
            visitor = new SQLServerSchemaStatVisitor();
        } else if (type.toLowerCase().equals(DbType.db2.toString())) {
            visitor = new DB2SchemaStatVisitor();
        } else if (type.toLowerCase().equals(DbType.odps.toString())) {
            visitor = new OdpsSchemaStatVisitor();
        } else if (type.toLowerCase().equals(DbType.hive.toString())) {
            visitor = new HiveSchemaStatVisitor();
        } else if (type.toLowerCase().equals(DbType.h2.toString())) {
            visitor = new H2SchemaStatVisitor();
        } else {
            throw new AppSystemException("暂时不支持的数据库类型操作");
        }
        stmt.accept(visitor);
        return visitor;
    }

    public static List<String> getSqlTableList(String sql, String type) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, type);
        List<String> tableList = new ArrayList<>();
        for (SQLStatement sqlStatement : stmtList) {
            SchemaStatVisitor visitor = getVisitor(sqlStatement, type);
            visitor.getTables().keySet().forEach(item -> {
                if (!tableList.contains(item.toString())) {
                    tableList.add(item.toString());
                }
            });
        }
        return tableList;
    }

    public static Map<String, String> getSqlManipulation(String sql, String type) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, type);
        Map<String, String> tableMap = new HashMap<>();
        for (SQLStatement sqlStatement : stmtList) {
            SchemaStatVisitor visitor = getVisitor(sqlStatement, type);
            visitor.getTables().forEach((k, v) -> {
                if (!tableMap.containsKey(k.toString())) {
                    tableMap.put(k.toString(), v.toString().toUpperCase());
                }
            });
        }
        return tableMap;
    }

    public static List<String> getSqlConditions(String sql, String type) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, type);
        List<String> tableList = new ArrayList<>();
        for (SQLStatement sqlStatement : stmtList) {
            SchemaStatVisitor visitor = getVisitor(sqlStatement, type);
            visitor.getConditions().forEach(itme -> {
                if (!itme.getValues().isEmpty()) {
                    tableList.add(itme.toString());
                }
            });
        }
        return tableList;
    }

    public static List<String> getRelationships(String sql, String type) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, type);
        List<String> tableList = new ArrayList<>();
        for (SQLStatement sqlStatement : stmtList) {
            SchemaStatVisitor visitor = getVisitor(sqlStatement, type);
            visitor.getRelationships().forEach(itme -> {
                if (!tableList.contains(itme.toString())) {
                    tableList.add(itme.toString());
                }
            });
        }
        return tableList;
    }

    public static Map<String, List<String>> getTableColumns(String sql, String type) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, type);
        Map<String, List<String>> targetTableMap = new LinkedHashMap<>();
        stmtList.forEach(sqlStatement -> {
            SchemaStatVisitor visitor = getVisitor(sqlStatement, type);
            Collection<TableStat.Column> columns = visitor.getColumns();
            columns.forEach(column -> {
                String fieldName = column.getName();
                if (!"UNKNOWN".equals(column.getTable())) {
                    if (targetTableMap.containsKey(column.getTable())) {
                        targetTableMap.get(column.getTable()).add(fieldName);
                    } else {
                        List<String> fieldList = new ArrayList<>();
                        fieldList.add(fieldName);
                        targetTableMap.put(column.getTable(), fieldList);
                    }
                }
            });
        });
        return targetTableMap;
    }
}
