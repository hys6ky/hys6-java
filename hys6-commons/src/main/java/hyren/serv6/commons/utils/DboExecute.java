package hyren.serv6.commons.utils;

import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;

@Slf4j
public class DboExecute {

    public static void updatesOrThrow(final String errorMsg, final String sql, Object... params) {
        updatesOrThrow(1, errorMsg, sql, params);
    }

    public static void updatesOrThrowNoMsg(final String sql, Object... params) {
        updatesOrThrow(1, "数据修改失败，修改了0条或多条数据", sql, params);
    }

    public static void updatesOrThrow(final int expectedUpdateNums, final String errorMsg, final String sql, Object... params) {
        int nums = Dbo.execute(sql, params);
        if (nums == expectedUpdateNums)
            return;
        String goodSql = formatSQL(sql, params);
        log.error(String.format("%s Illegal update for SQL[%s], expected %d but %d", ContextDataHolder.getBizId(), goodSql, expectedUpdateNums, nums));
        if (nums == 0) {
            throw new BusinessException(errorMsg == null ? "update nothing!" : errorMsg);
        } else {
            throw new BusinessException(errorMsg == null ? "Illegal update!" : errorMsg);
        }
    }

    public static void updatesOrThrowNoMsg(final int expectedUpdateNums, final String sql, Object... params) {
        updatesOrThrow(expectedUpdateNums, "数据修改失败", sql, params);
    }

    public static void insertsOrThrow(final String errorMsg, final String sql, Object... params) {
        int nums = Dbo.execute(sql, params);
        if (nums == 1)
            return;
        String goodSql = formatSQL(sql, params);
        log.error(String.format("%s Illegal insert for SQL[%s], expected %d but %d", ContextDataHolder.getBizId(), goodSql, 1, nums));
        if (nums == 0) {
            throw new BusinessException(errorMsg == null ? "insert nothing!" : errorMsg);
        } else {
            throw new BusinessException(errorMsg == null ? "Too many insert!" : errorMsg);
        }
    }

    public static void insertsOrThrowNoMsg(final String sql, Object... params) {
        insertsOrThrow("数据新增失败，返回数据不等于1", sql, params);
    }

    public static void deletesOrThrow(final String errorMsg, final String sql, Object... params) {
        deletesOrThrow(1, errorMsg, sql, params);
    }

    public static void deletesOrThrowNoMsg(final String sql, Object... params) {
        deletesOrThrow(1, "数据删除失败，删除了0条或多条数据", sql, params);
    }

    public static void deletesOrThrow(final int expectedDeleteNums, final String errorMsg, final String sql, Object... params) {
        int nums = Dbo.execute(sql, params);
        if (nums == expectedDeleteNums)
            return;
        String goodSql = formatSQL(sql, params);
        log.error(String.format("%s Illegal delete for SQL[%s], expected %d but %d", ContextDataHolder.getBizId(), goodSql, expectedDeleteNums, nums));
        if (nums == 0) {
            throw new BusinessException(errorMsg == null ? "delete nothing!" : errorMsg);
        } else {
            throw new BusinessException(errorMsg == null ? "Illegal delete!" : errorMsg);
        }
    }

    public static void deletesOrThrowNoMsg(final int expectedDeleteNums, final String sql, Object... params) {
        deletesOrThrow(expectedDeleteNums, "数据删除失败", sql, params);
    }

    public static String formatSQL(final String sql, Object... params) {
        return "SQL=[" + sql + "], Params=" + Arrays.toString(params);
    }

    private DboExecute() {
    }
}
