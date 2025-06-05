package hyren.serv6.h.process_flink.bean;

import java.util.Iterator;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class FlinkSqlUtil {

    private static String FROM = "FROM";

    public static String addDefaultValueColumn(String sql, String columnName, String value) {
        int i = sql.toUpperCase().indexOf(FROM);
        String newSql = sql.substring(0, i) + ", '" + value + "' AS " + columnName + " " + sql.substring(i, sql.length());
        return newSql;
    }

    public static String getSimpleTableName(String sql, String tableName) throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse(sql);
        if (statement instanceof Select) {
            Select selectStatement = (Select) statement;
            SelectBody selectBody = selectStatement.getSelectBody();
            if (selectBody instanceof PlainSelect) {
                PlainSelect plainSelect = (PlainSelect) selectBody;
                Table table = (Table) plainSelect.getFromItem();
                List<Join> joins = plainSelect.getJoins();
                if (tableName.equals(table.getFullyQualifiedName()) && table.getAlias() != null) {
                    return table.getAlias().getName();
                }
                if (joins != null) {
                    for (Join join : joins) {
                        if (join.getRightItem() instanceof Table) {
                            Table joinTable = (Table) join.getRightItem();
                            if (tableName.equals(joinTable.getFullyQualifiedName()) && table.getAlias() != null) {
                                return joinTable.getAlias().getName();
                            }
                        }
                    }
                }
            }
        }
        return tableName;
    }

    public static String addWhere(String sql, String where) throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse(sql);
        if (statement instanceof Select) {
            Select selectStatement = (Select) statement;
            SelectBody selectBody = selectStatement.getSelectBody();
            if (selectBody instanceof PlainSelect) {
                PlainSelect plainSelect = (PlainSelect) selectBody;
                if (plainSelect.getWhere() == null) {
                    plainSelect.setWhere(CCJSqlParserUtil.parseCondExpression(where));
                } else {
                    plainSelect.setWhere(new AndExpression(plainSelect.getWhere(), CCJSqlParserUtil.parseCondExpression(where)));
                }
                return plainSelect.toString();
            }
        }
        return null;
    }

    public static void main(String[] args) throws JSQLParserException {
        String sql = "select" + "  CS_LINKMAN," + "  CS_LICENCE_CODE," + "  CS_LETTER_CODE," + "  CS_POSTCODE," + "  CS_CSP_ID " + " from" + "  SSCJ_task01_CS01_A01_CSP_001";
        System.out.println(getSimpleTableName(sql, "SSCJ_task01_CS01_A01_CSP_001"));
    }
}
