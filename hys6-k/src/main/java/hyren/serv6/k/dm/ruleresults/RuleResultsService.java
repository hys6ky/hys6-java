package hyren.serv6.k.dm.ruleresults;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.base.entity.DqIndex3record;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.fileutil.FileDownloadUtil;
import hyren.serv6.k.dm.ruleresults.bean.RuleResultSearchBean;
import hyren.serv6.k.entity.DqDefinition;
import hyren.serv6.k.entity.DqResult;
import hyren.serv6.k.utils.CheckBeanUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class RuleResultsService {

    private static final Logger logger = LogManager.getLogger();

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getRuleResultInfos(Integer currPage, Integer pageSize) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        Page page = new DefaultPageImpl(currPage, pageSize);
        asmSql.clean();
        asmSql.addSql("SELECT t1.verify_date, t1.start_date, t1.start_time, t1.verify_result,t1.exec_mode,t1.dl_stat," + " t1.task_id, t2.target_tab, t2.reg_name,t2.reg_num, t2.flags, t2.rule_src, t2.rule_tag FROM " + DqResult.TableName + " t1 JOIN " + DqDefinition.TableName + " t2 ON t1.reg_num = t2.reg_num " + " where user_id=? ORDER BY t1.verify_date DESC").addParam(UserUtil.getUserId());
        List<Map<String, Object>> rule_result_s = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        Map<String, Object> ruleResultMap = new HashMap<>();
        ruleResultMap.put("rule_result_s", rule_result_s);
        ruleResultMap.put("totalSize", page.getTotalSize());
        return ruleResultMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ruleResultSearchBean", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchRuleResultInfos(RuleResultSearchBean ruleResultSearchBean, Integer currPage, Integer pageSize) {
        if (CheckBeanUtil.checkFullNull(ruleResultSearchBean)) {
            return getRuleResultInfos(currPage, pageSize);
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        Page page = new DefaultPageImpl(currPage, pageSize);
        asmSql.clean();
        asmSql.addSql("SELECT t1.verify_date, t1.start_date, t1.start_time, t1.verify_result,t1.exec_mode, t1.dl_stat," + " t1.task_id, t2.target_tab, t2.reg_name,t2.reg_num, t2.flags, t2.rule_src, t2.rule_tag FROM " + DqResult.TableName + " t1 JOIN " + DqDefinition.TableName + " t2 ON t1.reg_num = t2.reg_num" + " where t2.user_id=?").addParam(UserUtil.getUserId());
        if (StringUtil.isNotBlank(ruleResultSearchBean.getVerify_date())) {
            asmSql.addSql(" and t1.verify_date = ?").addParam(ruleResultSearchBean.getVerify_date());
        }
        if (StringUtil.isNotBlank(ruleResultSearchBean.getStart_date())) {
            asmSql.addSql(" and t1.start_date = ?").addParam(ruleResultSearchBean.getStart_date());
        }
        if (StringUtil.isNotBlank(ruleResultSearchBean.getRule_src())) {
            asmSql.addLikeParam(" t2.rule_src", '%' + ruleResultSearchBean.getRule_src() + '%');
        }
        if (StringUtil.isNotBlank(ruleResultSearchBean.getRule_tag())) {
            asmSql.addLikeParam(" t2.rule_tag", '%' + ruleResultSearchBean.getRule_tag() + '%');
        }
        if (StringUtil.isNotBlank(ruleResultSearchBean.getReg_name())) {
            asmSql.addLikeParam(" t2.reg_name", '%' + ruleResultSearchBean.getReg_name() + '%');
        }
        if (StringUtil.isNotBlank(ruleResultSearchBean.getReg_num())) {
            asmSql.addLikeParam(" cast(t1.reg_num as varchar(100))", '%' + ruleResultSearchBean.getReg_num() + '%');
        }
        if (null != ruleResultSearchBean.getCase_type() && ruleResultSearchBean.getCase_type().length > 0) {
            asmSql.addORParam("t1.case_type", ruleResultSearchBean.getCase_type());
        }
        if (null != ruleResultSearchBean.getExec_mode() && ruleResultSearchBean.getExec_mode().length > 0) {
            asmSql.addORParam("t1.exec_mode", ruleResultSearchBean.getExec_mode());
        }
        if (null != ruleResultSearchBean.getVerify_result() && ruleResultSearchBean.getVerify_result().length > 0) {
            asmSql.addORParam("t1.verify_result", ruleResultSearchBean.getVerify_result());
        }
        asmSql.addSql(" ORDER BY t1.verify_date DESC ");
        List<Map<String, Object>> rule_result_s = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        Map<String, Object> search_result_map = new HashMap<>();
        search_result_map.put("rule_result_s", rule_result_s);
        search_result_map.put("totalSize", page.getTotalSize());
        return search_result_map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "task_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public DqResult getRuleDetectDetail(String task_id) {
        Validator.notBlank(task_id, "查看的任务编号为空!");
        DqResult dq_result = new DqResult();
        dq_result.setTask_id(Long.valueOf(task_id));
        return Dbo.queryOneObject(DqResult.class, "SELECT * FROM " + DqResult.TableName + " WHERE task_id = ?", dq_result.getTask_id()).orElseThrow(() -> (new BusinessException("任务执行详细信息的SQL失败!")));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "reg_num", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getRuleExecuteHistoryInfo(long reg_num, int currPage, int pageSize) {
        if (StringUtil.isBlank(String.valueOf(reg_num))) {
            throw new BusinessException("规则编号为空!");
        }
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DqResult> dq_result_s = Dbo.queryPagedList(DqResult.class, page, "SELECT * FROM dq_result WHERE" + " reg_num = ? ORDER BY verify_date DESC", reg_num);
        Map<String, Object> dq_result_map = new HashMap<>();
        dq_result_map.put("dq_result_s", dq_result_s);
        dq_result_map.put("totalSize", page.getTotalSize());
        return dq_result_map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "task_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public void exportIndicator3Results(long task_id) {
        Validator.notBlank(String.valueOf(task_id), "导出指标3结果时,需要的任务id为空!");
        DqIndex3record di3 = new DqIndex3record();
        di3.setTask_id(task_id);
        DqResult dr = Dbo.queryOneObject(DqResult.class, "select * from " + DqResult.TableName + " where task_id=?", di3.getTask_id()).orElseThrow(() -> (new BusinessException("获取任务指标3存储记录的SQL异常!")));
        di3 = Dbo.queryOneObject(DqIndex3record.class, "select * from " + DqIndex3record.TableName + " where task_id=?", di3.getTask_id()).orElseThrow(() -> (new BusinessException("获取任务指标3存储记录的SQL异常!")));
        String sql = "select * from " + di3.getTable_name();
        List<Map<String, Object>> check_index3_list = new ArrayList<>();
        List<String> cols;
        try {
            cols = new ProcessingData() {

                @Override
                public void dealLine(Map<String, Object> map) {
                    check_index3_list.add(map);
                }
            }.getDataLayer(sql, Dbo.db());
        } catch (Exception e) {
            throw new BusinessException("获取指标3存储记录数据失败!" + e.getMessage());
        }
        List<Object[]> data_list = new ArrayList<>();
        check_index3_list.forEach(ci3 -> {
            Object[] o_arr = new Object[ci3.size()];
            for (int i = 0; i < cols.size(); i++) {
                o_arr[i] = ci3.get(cols.get(i));
            }
            data_list.add(o_arr);
        });
        String fileName = dr.getTarget_tab() + "_" + dr.getVerify_date() + ".csv";
        String savePath = WebinfoProperties.FileUpload_SavedDirName + File.separator + fileName;
        File file = new File(savePath);
        if (!file.exists()) {
            try {
                if (!file.createNewFile()) {
                    throw new BusinessException("创建文件失败,请检查是否拥有目录写权限！");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        CsvListWriter writer = null;
        try {
            writer = new CsvListWriter(new OutputStreamWriter(new FileOutputStream(file), DataBaseCode.UTF_8.getValue()), CsvPreference.EXCEL_PREFERENCE);
            writer.write(cols);
            long counter = 0;
            for (Object[] objects : data_list) {
                counter++;
                writer.write(objects);
                if (counter % 50000 == 0) {
                    logger.info("正在写入文件，已写入" + counter + "行");
                    writer.flush();
                }
            }
            logger.info("文件写入完成，写入" + counter + "行");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
            throw new BusinessException("创建文件流失败!");
        } finally {
            try {
                if (writer != null)
                    writer.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("关闭输出流失败!");
            }
        }
        FileDownloadUtil.downloadFile(savePath);
    }
}
