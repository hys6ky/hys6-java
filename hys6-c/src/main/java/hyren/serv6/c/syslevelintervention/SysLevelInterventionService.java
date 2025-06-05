package hyren.serv6.c.syslevelintervention;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.jdbc.SqlOperator.Assembler;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.Job_Status;
import hyren.serv6.base.codes.Main_Server_Sync;
import hyren.serv6.base.codes.Meddle_status;
import hyren.serv6.base.codes.Meddle_type;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.jobUtil.EtlJobUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class SysLevelInterventionService {

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> searchSysLevelCurrInterventionInfo(Long etl_sys_id, Long userId) {
        if (EtlJobUtil.isEtlSysExistById(etl_sys_id, userId, Dbo.db())) {
            throw new BusinessException("当前工程已不存在");
        }
        return Dbo.queryList("SELECT t1.event_id,t4.etl_sys_id,t4.etl_sys_cd,t2.etl_job_id,t2.etl_job,t1.etl_hand_type," + "t1.pro_para,hand_status,st_time,warning FROM " + EtlJobHand.TableName + " t1 left join " + EtlJobDef.TableName + " t2 on t1.etl_job_id=t2.etl_job_id " + " and t1.etl_sys_id=t2.etl_sys_id left join " + EtlSubSysList.TableName + " t3 on t2.sub_sys_id=t3.sub_sys_id and t2.etl_sys_id=t3.etl_sys_id" + " LEFT JOIN " + EtlSys.TableName + " t4 on t1.etl_sys_id = t4.etl_sys_id " + " WHERE t1.etl_sys_id=? AND t1.etl_job_id=?", etl_sys_id, 1000000000000000000L);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> searchSysLeverHisInterventionByPage(Long etl_sys_id, int currPage, int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> handHisList = Dbo.queryPagedList(page, "SELECT t1.event_id,t4.etl_sys_id,t4.etl_sys_cd,t2.etl_job_id,t2.etl_job,t1.etl_hand_type,t1.pro_para,hand_status,st_time,warning " + "FROM " + EtlJobHandHis.TableName + " t1 left join " + EtlJobCur.TableName + " t2 on t1.etl_job_id=t2.etl_job_id and t1.etl_sys_id=t2.etl_sys_id left join " + EtlSubSysList.TableName + " t3 on t2.sub_sys_id=t3.sub_sys_id " + " and t2.etl_sys_id=t3.etl_sys_id " + " left join " + EtlSys.TableName + " t4 on t1.etl_sys_id=t4.etl_sys_id " + " WHERE t1.etl_sys_id=?", etl_sys_id);
        Map<String, Object> handHisMap = new HashMap<>();
        handHisMap.put("handHisList", handHisList);
        handHisMap.put("totalSize", page.getTotalSize());
        return handHisMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchSystemBatchConditions(Long etl_sys_id) {
        List<String> currBathDateList = Dbo.queryOneColumnList("select curr_bath_date from " + EtlSys.TableName + " where etl_sys_id=?", etl_sys_id);
        String curr_bath_date = currBathDateList.get(0);
        List<Map<String, Object>> etlJobCurrList = Dbo.queryList("select sum(case when job_disp_status=? then 1 else 0 end) as done_num ," + "sum(case when job_disp_status=? then 1 else 0 end) as pending_num,sum(case when " + " job_disp_status='A' then 1 else 0 end) as alarm_num,sum(case when " + " job_disp_status=? then 1 else 0 end) as error_num,sum(case when " + " job_disp_status=? then 1 else 0 end) as waiting_num,sum(case when " + " job_disp_status=? then 1 else 0 end) as running_num,sum(case when " + " job_disp_status=? then 1 else 0 end) as stop_num from " + EtlJobCur.TableName + " where etl_sys_id=?", Job_Status.DONE.getCode(), Job_Status.PENDING.getCode(), Job_Status.ERROR.getCode(), Job_Status.WAITING.getCode(), Job_Status.RUNNING.getCode(), Job_Status.STOP.getCode(), etl_sys_id);
        Map<String, Object> etlJobCurrMap = new HashMap<>();
        etlJobCurrMap.put("curr_bath_date", curr_bath_date);
        etlJobCurrMap.put("etlJobCurrList", etlJobCurrList);
        return etlJobCurrMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_hand_type", desc = "", range = "")
    @Param(name = "curr_bath_date", desc = "", range = "")
    public void sysLevelInterventionOperate(Long etl_sys_id, String etl_hand_type, String curr_bath_date, Long userId) {
        Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select * from " + EtlSys.TableName + " where etl_sys_id=? and user_id=?");
        assembler.addParam(etl_sys_id);
        assembler.addParam(userId);
        EtlSys etlSys = SqlOperator.queryOneObject(Dbo.db(), EtlSys.class, assembler.sql(), assembler.params()).orElseThrow(() -> (new BusinessException("当前工程已不存在")));
        EtlJobHand etl_job_hand = new EtlJobHand();
        etl_job_hand.setEtl_sys_id(etlSys.getEtl_sys_id());
        etl_job_hand.setEtl_job_id(1000000000000000000L);
        etl_job_hand.setEtl_hand_type(etl_hand_type);
        etl_job_hand.setHand_status(Meddle_status.TRUE.getCode());
        etl_job_hand.setMain_serv_sync(Main_Server_Sync.YES.getCode());
        etl_job_hand.setEvent_id(DateUtil.parseStr2DateWith8Char(DateUtil.getSysDate()) + " " + DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()));
        etl_job_hand.setSt_time(DateUtil.parseStr2DateWith8Char(DateUtil.getSysDate()) + " " + DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()));
        etl_job_hand.setPro_para(etlSys.getEtl_sys_cd() + "," + curr_bath_date);
        if (EtlJobUtil.isEtlJobHandExist(etlSys.getEtl_sys_id(), Dbo.db())) {
            throw new BusinessException("工程下有作业正在干预！");
        }
        Meddle_type.ofEnumByCode(etl_hand_type);
        if (Meddle_type.SYS_ORIGINAL == (Meddle_type.ofEnumByCode(etl_hand_type)) || Meddle_type.SYS_RESUME == (Meddle_type.ofEnumByCode(etl_hand_type))) {
            String[] jobStatus = { Job_Status.PENDING.getCode(), Job_Status.RUNNING.getCode(), Job_Status.WAITING.getCode(), Job_Status.ERROR.getCode() };
            long count = getEtlSysStatus(etlSys.getEtl_sys_id(), jobStatus);
            if (count > 0) {
                Dbo.rollbackTransaction();
                throw new BusinessException("工程状态为错误,挂起,运行或等待的不可以重跑!");
            } else {
                etl_job_hand.add(Dbo.db());
            }
        } else {
            etl_job_hand.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "jobStatus", desc = "", range = "")
    @Return(desc = "", range = "")
    private long getEtlSysStatus(Long etl_sys_id, String[] jobStatus) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT count(1) from " + EtlSys.TableName + " where etl_sys_id=?");
        asmSql.addParam(etl_sys_id);
        if (jobStatus != null && jobStatus.length > 0) {
            asmSql.addORParam("sys_run_status", jobStatus);
        }
        return Dbo.queryNumber(asmSql.sql(), asmSql.params()).orElseThrow(() -> new BusinessException("sql查询错误！"));
    }
}
