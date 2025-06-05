package hyren.serv6.k.dm;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DqcVerifyResult;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DtabRelationStore;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.k.entity.DqResult;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DataManageService {

    @Method(desc = "", logicStep = "")
    @Param(name = "statistics_layer_num", desc = "", range = "", valueIfNull = "6")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getTableStatistics(int statistics_layer_num) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select dsl.dsl_id,dsl.dsl_name,count(dtrs.tab_id) from " + DataStoreLayer.TableName + " dsl" + " left join " + DtabRelationStore.TableName + " dtrs on dsl.dsl_id=dtrs.dsl_id group by dsl.dsl_id" + " order by count desc");
        if (statistics_layer_num > 0) {
            asmSql.addSql(" limit ?").addParam(statistics_layer_num);
        }
        return Dbo.queryList(asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getRuleStatistics() {
        Map<String, Object> ruleStatisticsMap = new HashMap<>();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT COUNT(1) AS check_passes_number FROM " + DqResult.TableName + " WHERE verify_result = ?").addParam(DqcVerifyResult.ZhengChang.getCode());
        long check_passes_number = Dbo.queryNumber(asmSql.sql(), asmSql.params()).orElseThrow(() -> new BusinessException("统计检查通过数的SQL错误!"));
        ruleStatisticsMap.put("check_passes_number", String.valueOf(check_passes_number));
        asmSql.clean();
        asmSql.addSql("SELECT COUNT(1) AS check_exception_number FROM " + DqResult.TableName + " WHERE verify_result = ?").addParam(DqcVerifyResult.YiChang.getCode());
        long check_exception_number = Dbo.queryNumber(asmSql.sql(), asmSql.params()).orElseThrow(() -> new BusinessException("统计检查异常数的SQL错误!"));
        ruleStatisticsMap.put("check_exception_number", String.valueOf(check_exception_number));
        asmSql.clean();
        asmSql.addSql("SELECT COUNT(1) AS execution_failed_number FROM " + DqResult.TableName + " WHERE verify_result = ?").addParam(DqcVerifyResult.ZhiXingShiBai.getCode());
        long execution_failed_number = Dbo.queryNumber(asmSql.sql(), asmSql.params()).orElseThrow(() -> new BusinessException("统计执行失败数的SQL错误!"));
        ruleStatisticsMap.put("execution_failed_number", String.valueOf(execution_failed_number));
        asmSql.clean();
        asmSql.addSql("SELECT COUNT(1) AS rule_total_number FROM " + DqResult.TableName);
        long rule_total_number = Dbo.queryNumber(asmSql.sql(), asmSql.params()).orElseThrow(() -> new BusinessException("统计执行失败数的SQL错误!"));
        ruleStatisticsMap.put("rule_total_number", String.valueOf(rule_total_number));
        asmSql.clean();
        asmSql.addSql("SELECT task_id, verify_date, target_tab FROM " + DqResult.TableName + " WHERE verify_result = ? " + "ORDER BY verify_date DESC limit 5").addParam(DqcVerifyResult.ZhengChang.getCode());
        List<Map<String, Object>> check_passes_top5 = Dbo.queryList(asmSql.sql(), asmSql.params());
        ruleStatisticsMap.put("check_passes_top5", check_passes_top5);
        asmSql.clean();
        asmSql.addSql("SELECT task_id, verify_date, target_tab FROM " + DqResult.TableName + " WHERE verify_result = ? " + "ORDER BY verify_date DESC limit 5").addParam(DqcVerifyResult.YiChang.getCode());
        List<Map<String, Object>> check_exception_top5 = Dbo.queryList(asmSql.sql(), asmSql.params());
        ruleStatisticsMap.put("check_exception_top5", check_exception_top5);
        asmSql.clean();
        asmSql.addSql("SELECT task_id, verify_date, target_tab FROM " + DqResult.TableName + " WHERE verify_result = ? " + "ORDER BY verify_date DESC limit 5").addParam(DqcVerifyResult.ZhiXingShiBai.getCode());
        List<Map<String, Object>> execution_failed_top5 = Dbo.queryList(asmSql.sql(), asmSql.params());
        ruleStatisticsMap.put("execution_failed_top5", execution_failed_top5);
        return ruleStatisticsMap;
    }
}
