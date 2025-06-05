package hyren.serv6.r.audit.service.impl;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.r.audit.service.TableApplyService;
import hyren.serv6.r.audit.web.dto.PageDTO;
import hyren.serv6.r.audit.web.dto.TableApplySyncInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class TableApplyServiceImpl implements TableApplyService {

    @Override
    public PageDTO<TableApplySyncInfo> page(Long dfPid, String name, String status, Page page) {
        List<Object> params = new ArrayList<>();
        SqlOperator.Assembler dataSql = SqlOperator.Assembler.newInstance();
        dataSql.addSql("SELECT dta.apply_tab_id, dta.table_id,dta.df_pid,dta.dep_id,dta.create_user_id,dta.create_date,dta.create_time,\n" + " dta.update_date,dta.update_time,dta.dta_remarks,dta.dsl_table_name_id,dta.is_sync,dta.is_rec,dsr.original_name as hyren_name,\n" + " ti.table_ch_name,su.user_name,di.dep_name FROM df_table_apply dta\n" + " LEFT JOIN table_info ti ON dta.table_id = ti.table_id\n" + " LEFT JOIN table_storage_info tsi ON dta.table_id = tsi.table_id\n" + " LEFT JOIN sys_user su ON dta.create_user_id = su.user_id\n" + " LEFT JOIN department_info di ON dta.dep_id = di.dep_id \n" + " JOIN data_store_reg dsr ON dsr.table_id = tsi.table_id\n" + " JOIN database_set dbs ON dsr.database_id = dbs.database_id");
        SqlOperator.Assembler check = check(dfPid, name, status, dataSql, true);
        dataSql.addSql(" UNION ");
        dataSql.addSql(" SELECT dta.apply_tab_id,dta.table_id,dta.df_pid,dta.dep_id,dta.create_user_id,dta.create_date,dta.create_time,\n" + " dta.update_date,dta.update_time,dta.dta_remarks,dta.dsl_table_name_id,dta.is_sync,dta.is_rec,tsi.hyren_name,\n" + " ti.table_ch_name,su.user_name,di.dep_name FROM df_table_apply dta\n" + " LEFT JOIN table_info ti ON dta.table_id = ti.table_id\n" + " LEFT JOIN table_storage_info tsi ON dta.table_id = tsi.table_id\n" + " LEFT JOIN sys_user su ON dta.create_user_id = su.user_id\n" + " LEFT JOIN department_info di ON dta.dep_id = di.dep_id \n" + " JOIN data_store_reg dsr ON dsr.table_id = tsi.table_id\n" + " JOIN database_set dbs ON dsr.database_id = dbs.database_id");
        SqlOperator.Assembler result = check(dfPid, name, status, check, false);
        try {
            List<TableApplySyncInfo> list = SqlOperator.queryPagedList(Dbo.db(), TableApplySyncInfo.class, page, result.sql(), result.params().toArray());
            return new PageDTO<TableApplySyncInfo>(list, page.getTotalSize());
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException(e.getMessage());
        }
    }

    private SqlOperator.Assembler check(Long dfPid, String name, String status, SqlOperator.Assembler dataSql, boolean flag) {
        boolean hasWhere = false;
        boolean needAnd = false;
        if (dfPid != null) {
            if (!hasWhere) {
                dataSql.addSql(" where ");
                hasWhere = true;
            }
            if (needAnd) {
                dataSql.addSql(" and ");
            } else {
                needAnd = true;
            }
            if (flag) {
                dataSql.addSql(" dbs.collect_type = '1' AND dsr.hyren_name = tsi.hyren_name AND dsr.collect_type IN ( '4', '1' ) and dta.df_pid = ? ");
            } else {
                dataSql.addSql(" dbs.collect_type != '1' AND dsr.hyren_name = tsi.hyren_name AND dsr.collect_type IN ( '4', '1' ) and dta.df_pid = ? ");
            }
            dataSql.addParam(dfPid);
        }
        if (StringUtil.isNotBlank(name)) {
            if (!hasWhere) {
                dataSql.addSql(" where ");
                hasWhere = true;
            }
            if (needAnd) {
                dataSql.addSql(" and ");
            } else {
                needAnd = true;
            }
            if (flag) {
                dataSql.addSql(" dbs.collect_type = '1' AND dsr.hyren_name = tsi.hyren_name AND dsr.collect_type IN ( '4', '1' ) AND dsr.original_name like ? ");
            } else {
                dataSql.addSql(" dbs.collect_type != '1' AND dsr.hyren_name = tsi.hyren_name AND dsr.collect_type IN ( '4', '1' ) AND tsi.hyren_name like ? ");
            }
            dataSql.addParam("'%" + name + "%'");
        }
        if (StringUtil.isNotBlank(status)) {
            if (!hasWhere) {
                dataSql.addSql(" where ");
                hasWhere = true;
            }
            if (needAnd) {
                dataSql.addSql(" and ");
            } else {
                needAnd = true;
            }
            dataSql.addSql(" dta.is_sync = ? ");
            dataSql.addParam(status);
        }
        return dataSql;
    }
}
