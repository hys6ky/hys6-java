package hyren.serv6.r.audit.service.impl;

import fd.ng.core.utils.DateUtil;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DFAppState;
import hyren.serv6.base.entity.DfAuditOpinion;
import hyren.serv6.base.entity.DfProInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.r.audit.service.ProInfoService;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ProInfoServiceImpl implements ProInfoService {

    @Override
    public Boolean passList(List<Long> dfPids) {
        String inC = dfPids.stream().map(t -> "?").collect(Collectors.joining(","));
        String sql = "UPDATE " + DfProInfo.TableName + " SET submit_state='" + DFAppState.YiShenPi.getCode() + "',audit_date='" + DateUtil.getSysDate() + "',audit_time='" + DateUtil.getSysTime() + "' WHERE df_pid in (" + inC + ")";
        int execute = SqlOperator.execute(Dbo.db(), sql, dfPids.toArray());
        if (dfPids.size() != execute) {
            throw new BusinessException("通过数据数量不符，请刷新页面并重试");
        }
        return true;
    }

    @Override
    public Boolean refuseList(List<Long> dfPids, String audit_opinion, String remarks) {
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("UPDATE " + DfProInfo.TableName + " SET submit_state='" + DFAppState.YiJuJue.getCode() + "' WHERE ");
        sql.addORParam("df_pid", dfPids.toArray(), "");
        int execute = SqlOperator.execute(Dbo.db(), sql.sql(), sql.params());
        if (dfPids.size() != execute) {
            throw new BusinessException("通过数据数量不符，请刷新页面并重试");
        }
        String insertSql = "INSERT INTO " + DfAuditOpinion.TableName + "(DF_PID, AUDIT_ID, AUDIT_OPINION, AUDIT_REMARKS) VALUES(?,?,?,?)";
        List<Object[]> params = dfPids.stream().map(proId -> (new Object[] { proId, PrimayKeyGener.getNextId(), audit_opinion, remarks })).collect(Collectors.toList());
        int[] executeArray = SqlOperator.executeBatch(Dbo.db(), insertSql, params);
        for (int i : executeArray) {
            if (i != 1) {
                throw new BusinessException("新增审批信息失败");
            }
        }
        return true;
    }
}
