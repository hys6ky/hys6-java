package hyren.serv6.a.bigscreendisplay;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.utils.agentmonitor.AgentMonitorUtil;
import hyren.serv6.commons.utils.constant.Constant;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service("bigScreenDisplayService")
public class BigScreenDisplayService {

    public long querylong() {
        return Dbo.queryList("SELECT etl_job FROM " + EtlJobCur.TableName + " WHERE split_part(curr_st_time, ' ', 1) = ? AND job_disp_status = ? " + " AND etl_sys_cd in (SELECT etl_sys_cd FROM " + EtlSys.TableName + " ) GROUP BY etl_job", DateUtil.getSysDate(), Job_Status.ERROR.getCode()).size();
    }

    public String queryString(String yearData) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("select (case when collet_database_size = '' then '0' else collet_database_size end) " + "collet_database_size  from " + CollectCase.TableName);
        if (StringUtil.isNotBlank(yearData)) {
            assembler.addSql(" WHERE collect_s_date like ? ").addParam(yearData + '%');
        }
        List<Object> list = Dbo.queryOneColumnList(assembler.sql(), assembler.params());
        long sum = list.stream().filter(Objects::nonNull).mapToLong(item -> Long.parseLong(item.toString())).sum();
        return FileUtil.fileSizeConversion(sum);
    }

    public long queryTotalLong(String yearData) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("SELECT  COUNT(1) FROM (SELECT count(1) countNum  FROM " + DataSource.TableName);
        if (StringUtil.isNotBlank(yearData)) {
            assembler.addSql(" WHERE create_date LIKE ?").addParam(yearData + "%");
        }
        assembler.addSql(" GROUP BY datasource_number) a");
        return Dbo.queryNumber(assembler.sql(), assembler.params()).orElse(0);
    }

    public long querytotalNumber() {
        return Dbo.queryNumber("SELECT COUNT(1) FROM (SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE collect_type = ? OR collect_type = ? GROUP BY database_type,database_name,database_ip,database_port) a", CollectType.ShuJuKuCaiJi.getCode(), CollectType.ShuJuKuChouShu.getCode()).orElse(0);
    }

    public Long totalNumberOfAccessDataTables(String yearData) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("SELECT COUNT(1) FROM " + TableInfo.TableName + " WHERE database_id IN  (SELECT t1.database_id FROM " + DatabaseSet.TableName + " t1 join " + AgentInfo.TableName + " t2 on t2.agent_id = t1.agent_id WHERE (t1.collect_type = ? OR t1.collect_type = ? ) ");
        assembler.addParam(CollectType.ShuJuKuCaiJi.getCode()).addParam(CollectType.ShuJuKuChouShu.getCode());
        if (StringUtil.isNotBlank(yearData)) {
            assembler.addSql(" AND t2.create_date like ?").addParam(yearData + '%');
        }
        assembler.addSql(")");
        return Dbo.queryNumber(assembler.sql(), assembler.params()).orElse(0);
    }

    public Map<String, Object> queryTotalNumberOfCollect() {
        Result result = Dbo.queryResult(" select sum((CASE WHEN collect_type = ? THEN file_size ELSE 0 END)) filecollectsize," + "sum((CASE WHEN collect_type in (?,?) THEN file_size ELSE 0 END)) dbcollectsize FROM " + DataStoreReg.TableName + " sfa JOIN  " + AgentInfo.TableName + " ai ON sfa.agent_id = ai.agent_id", AgentType.WenJianXiTong.getCode(), AgentType.ShuJuKu.getCode(), AgentType.DBWenJian.getCode());
        result.setObject(0, "filesize", FileUtil.fileSizeConversion(result.getLongDefaultZero(0, "filecollectsize")));
        result.setObject(0, "dbsize", FileUtil.fileSizeConversion(result.getLongDefaultZero(0, "dbcollectsize")));
        return result.toList().get(0);
    }

    public long queryNumberOfTableCollectedToday() {
        return Dbo.queryNumber("SELECT COUNT(1) FROM (select task_classify FROM " + CollectCase.TableName + " WHERE collect_s_date = ? AND (collect_type = ? OR collect_type = ?) GROUP BY task_classify) a", DateUtil.getSysDate(), AgentType.ShuJuKu.getCode(), AgentType.DBWenJian.getCode()).orElse(0);
    }

    public long numberOfFailedCollectionTablesToday() {
        return Dbo.queryNumber("SELECT COUNT(1) FROM (SELECT task_classify FROM " + CollectCase.TableName + " WHERE collect_s_date = ? AND (collect_type = ? OR collect_type = ?) AND collect_s_date = ? GROUP BY task_classify" + " HAVING POSITION(? in STRING_AGG(execute_state,'-')) != 0 ) a", DateUtil.getSysDate(), AgentType.ShuJuKu.getCode(), AgentType.DBWenJian.getCode(), DateUtil.getSysDate(), ExecuteState.YunXingShiBai.getCode()).orElse(0);
    }

    public long newAccessData() {
        List<CollectCase> collect_cases = Dbo.queryList(CollectCase.class, "SELECT colect_record,task_classify,collect_set_id FROM " + CollectCase.TableName + " WHERE  collect_set_id in (SELECT  database_id  FROM  " + TableInfo.TableName + " WHERE valid_s_date = ? AND valid_e_date = ? GROUP BY database_id ) AND collect_s_date = ? GROUP BY colect_record, " + "task_classify,collect_set_id HAVING POSITION( ? in STRING_AGG(execute_state,'-')) = 0 ", DateUtil.getSysDate(), Constant._MAX_DATE_8, DateUtil.getSysDate(), ExecuteState.YunXingWanCheng.getCode());
        return collect_cases.stream().mapToLong(CollectCase::getColect_record).sum();
    }

    public long numberOfTablesAfterProcessing() {
        return Dbo.queryNumber("SELECT COUNT(1) FROM (SELECT t2.is_successful FROM " + DmModuleTable.TableName + " t1 JOIN " + DtabRelationStore.TableName + " t2 ON t1.module_table_id = t2.tab_id where is_successful = ?) a", JobExecuteState.WanCheng.getCode()).orElse(0);
    }

    public List<Map<String, Object>> dataSchedulingSituation() {
        return Dbo.queryList("SELECT  t3.user_name,SPLIT_PART(pro_para, '@', 2)," + "( CASE job_disp_status " + "WHEN ? THEN '完成' " + "WHEN ? THEN '错误' " + "WHEN ? THEN '挂起' " + "WHEN ? THEN '运行' " + "WHEN ? THEN '停止'  " + "WHEN ? THEN '等待'  " + "ELSE '' END) AS job_disp_status FROM " + EtlJobCur.TableName + " t1 join etl_sys t2 ON t1.etl_sys_cd = t2.etl_sys_cd JOIN " + SysUser.TableName + " t3 on t2.user_id = t3.user_id WHERE SPLIT_PART(curr_st_time, ' ', 1) = ?", Job_Status.DONE.getCode(), Job_Status.ERROR.getCode(), Job_Status.PENDING.getCode(), Job_Status.RUNNING.getCode(), Job_Status.STOP.getCode(), Job_Status.WAITING.getCode(), DateUtil.getSysDate());
    }

    public List<Map<String, Object>> interfaceCallSituation() {
        return Dbo.queryList("SELECT  interface_name,user_name,split_part(request_stime, ' ', 1) AS split_part  FROM  " + InterfaceUseLog.TableName + " where split_part(request_stime, ' ', 1) = ?", DateUtil.getSysDate());
    }

    public long collectTotalSize() {
        return Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName).orElse(0L);
    }

    public Result datasourceAndCollectTotalSize() {
        return Dbo.queryResult("SELECT datasource_name,sum(colect_record)  FROM " + CollectCase.TableName + " t1 JOIN " + DataSource.TableName + " t2 on t1.source_id = t2.source_id  GROUP BY datasource_name");
    }

    public long numberOfInterfaceToday() {
        return Dbo.queryNumber("SELECT COUNT(1) FROM " + InterfaceUseLog.TableName + " WHERE request_etime like ?", DateUtil.getSysDate() + "%").orElse(0L);
    }

    public long newAccessDatabaseYear() {
        return Dbo.queryNumber("SELECT count(1) from (SELECT database_type,database_name,database_port,jdbc_url FROM " + DatabaseSet.TableName + " WHERE database_id IN (SELECT database_id  FROM " + DataStoreReg.TableName + "  WHERE collect_type = ? AND storage_date like ? GROUP BY database_id) " + " GROUP BY database_type,database_name,database_port,jdbc_url) a", AgentType.ShuJuKu.getCode(), DateUtil.getSysDate().substring(0, 4) + "%").orElse(0L);
    }

    public long numberOfFileToday() {
        return Dbo.queryNumber(" select count(1) FROM " + DataStoreReg.TableName + " t1 JOIN  " + AgentInfo.TableName + " t2 ON t1.agent_id = t2.agent_id WHERE storage_date = ? AND collect_type = ?", DateUtil.getSysDate(), AgentType.WenJianXiTong.getCode()).orElse(0L);
    }

    public long numberOfFileYear() {
        return Dbo.queryNumber(" select count(1) FROM " + DataStoreReg.TableName + " t1 JOIN  " + AgentInfo.TableName + " t2 ON t1.agent_id = t2.agent_id WHERE storage_date like ? AND collect_type = ?", DateUtil.getSysDate().substring(0, 4) + "%", AgentType.WenJianXiTong.getCode()).orElse(0L);
    }

    public String totalNumberOfData(String yearData) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.addSql("SELECT colect_record,task_classify  FROM " + CollectCase.TableName);
        if (StringUtil.isNotBlank(yearData)) {
            assembler.addSql(" WHERE collect_s_date LIKE ?").addParam(yearData + '%');
        }
        assembler.addSql(" GROUP BY colect_record,task_classify");
        List<Map<String, Object>> list = Dbo.queryList(assembler.sql(), assembler.params());
        long colect_record = list.stream().mapToLong(item -> Long.parseLong(item.get("colect_record").toString())).sum();
        return formatNumber(String.valueOf(colect_record));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "needFormatNum", desc = "", range = "")
    @Return(desc = "", range = "")
    private String formatNumber(String needFormatNum) {
        String unit = "";
        String formatNumStr = "";
        StringBuilder sb = new StringBuilder();
        BigDecimal bigDecimal = new BigDecimal("10000");
        BigDecimal bigDecimal2 = new BigDecimal("100000000");
        BigDecimal needNum = new BigDecimal(needFormatNum);
        if ((needNum.compareTo(bigDecimal) == 0 || needNum.compareTo(bigDecimal) > 0) && needNum.compareTo(bigDecimal2) < 0) {
            formatNumStr = needNum.divide(bigDecimal).setScale(3, RoundingMode.HALF_UP).toString();
            unit = "万";
        } else if (needNum.compareTo(bigDecimal2) >= 0) {
            formatNumStr = needNum.divide(bigDecimal2).setScale(3, RoundingMode.HALF_UP).toString();
            unit = "亿";
        } else {
            sb.append(needNum);
        }
        if (StringUtil.isNotBlank(formatNumStr)) {
            NumberFormat nf = NumberFormat.getInstance();
            nf.setGroupingUsed(false);
            double number = new BigDecimal(formatNumStr).doubleValue();
            sb.append(nf.format(number)).append(unit);
        }
        return sb.toString();
    }

    public List<Map<String, Object>> getResourceInfo(long startTime, long endTime) {
        List<AgentDownInfo> agent_down_infos = Dbo.queryList(AgentDownInfo.class, "SELECT t2.agent_name,t1.agent_ip,t1.agent_port,t2.agent_context,t2.agent_pattern FROM " + AgentInfo.TableName + " t1 JOIN " + AgentDownInfo.TableName + " t2 ON t1.agent_ip = t2.agent_ip AND t1.agent_port = t2.agent_port WHERE t1.agent_status = ? " + " GROUP BY t2.agent_name,t1.agent_ip,t1.agent_port,t2.agent_context,t2.agent_pattern", AgentStatus.YiLianJie.getCode());
        List<Map<String, Object>> array = new ArrayList<Map<String, Object>>();
        agent_down_infos.forEach(agent_down_info -> {
            Map<String, Object> object = JsonUtil.toObject(JsonUtil.toJson(agent_down_info), new TypeReference<Map<String, Object>>() {
            });
            ActionResult actionResult = AgentMonitorUtil.agentResourceInfo(agent_down_info, startTime, endTime);
            if (actionResult.getData() == null) {
                object.put("status", false);
            } else {
                List<Map<String, Object>> resourceData = JsonUtil.toObject(actionResult.getData().toString(), new TypeReference<List<Map<String, Object>>>() {
                });
                object.put("resource", resourceData);
                object.put("status", true);
            }
            array.add(object);
        });
        return array;
    }

    public long dataProcessingVolume() {
        List<DmJobTableInfo> dm_operation_infos = Dbo.queryList(DmJobTableInfo.class, "SELECT t2.jobtab_view_sql FROM " + DmModuleTable.TableName + " t1 JOIN " + DmJobTableInfo.TableName + " t2 ON t1.module_table_id = t2.module_table_id");
        return dm_operation_infos.stream().mapToLong(dm_operation_info -> DruidParseQuerySql.parseSqlTable(dm_operation_info.getJobtab_view_sql()).size()).sum();
    }
}
