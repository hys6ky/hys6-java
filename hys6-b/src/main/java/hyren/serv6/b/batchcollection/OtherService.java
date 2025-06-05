package hyren.serv6.b.batchcollection;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.EtlDependency;
import hyren.serv6.base.entity.EtlJobDef;
import hyren.serv6.base.entity.EtlJobResourceRela;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.jobUtil.EtlJobUtil;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class OtherService {

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> searchEtlJob(Long etl_sys_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        return Dbo.queryList("select etl_job,etl_job_id from " + EtlJobDef.TableName + " where etl_sys_id=?", etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Long> searchJobDependency(String etl_sys_cd, String[] etl_job) {
        Map<String, Long> map = new HashMap<>();
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.clean();
        assembler.addSql("select count(1) from " + EtlDependency.TableName);
        assembler.addSql("etl_sys_cd = ? ");
        assembler.addParam(etl_sys_cd);
        assembler.addORParam("and etl_job", etl_job);
        long dependcy_count = Dbo.queryNumber(assembler.sql(), assembler.params()).orElseThrow(() -> new BusinessException("查询失败!"));
        assembler.clean();
        assembler.addSql("select count(1) from " + EtlJobResourceRela.TableName);
        assembler.addSql("etl_sys_cd = ?").addParam(etl_sys_cd);
        assembler.addORParam("etl_job", etl_job);
        long resource_count = Dbo.queryNumber(assembler.sql(), assembler.params()).orElseThrow(() -> new BusinessException("查询失败!"));
        map.put("dependcy_count", dependcy_count);
        map.put("resource_count", resource_count);
        return map;
    }
}
