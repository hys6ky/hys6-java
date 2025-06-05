package hyren.serv6.b.batchcollection;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.entity.CollectCase;
import hyren.serv6.commons.enumtools.StageConstant;
import io.swagger.annotations.Api;
import org.springframework.stereotype.Component;
import java.util.*;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2019-11-01")
@Api("处理采集任务表的信息")
@Component
public class JobTableDetails {

    @Method(desc = "", logicStep = "")
    @Param(name = "collectJobList", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getTableDetails(List<CollectCase> collectJobList) {
        Map<String, Map<String, Object>> detailsMap = new LinkedHashMap<String, Map<String, Object>>();
        collectJobList.forEach(collect_case -> {
            String collect_s_date = DateUtil.parseStr2DateWith8Char(collect_case.getCollect_s_date()).toString() + ' ' + DateUtil.parseStr2TimeWith6Char(collect_case.getCollect_s_time()).toString();
            String collect_e_date = DateUtil.parseStr2DateWith8Char(String.valueOf((collect_case.getCollect_e_date()))).toString() + ' ' + DateUtil.parseStr2TimeWith6Char(StringUtil.isBlank(collect_case.getCollect_e_time()) ? "000001" : collect_case.getCollect_e_time()).toString();
            String table_name = collect_case.getTask_classify();
            String job_type = collect_case.getJob_type();
            String stateStr = null;
            if (StageConstant.UNLOADDATA.getCode() == Integer.parseInt(job_type)) {
                stateStr = "B001";
            } else if (StageConstant.UPLOAD.getCode() == Integer.parseInt(job_type)) {
                stateStr = "B002";
            } else if (StageConstant.DATALOADING.getCode() == Integer.parseInt(job_type)) {
                stateStr = "UHDFS";
            } else if (StageConstant.CALINCREMENT.getCode() == Integer.parseInt(job_type)) {
                stateStr = "LHIVE";
            } else {
                stateStr = "SOURCE";
            }
            if (detailsMap.containsKey(table_name)) {
                Map<String, Object> map = detailsMap.get(table_name);
                map.put(stateStr + "_S_TITLE", collect_s_date);
                map.put(stateStr + "_E_TITLE", collect_e_date);
                map.put(stateStr, collect_case.getExecute_state());
                map.put(stateStr + "error", collect_case.getCc_remark());
            } else {
                Map<String, Object> map = new HashMap<>();
                map.put("table_name", table_name);
                map.put(stateStr + "_S_TITLE", collect_s_date);
                map.put(stateStr + "_E_TITLE", collect_e_date);
                map.put(stateStr, collect_case.getExecute_state());
                map.put(stateStr + "error", collect_case.getCc_remark());
                detailsMap.put(table_name, map);
            }
        });
        List<Map<String, Object>> collectTableResult = new ArrayList<Map<String, Object>>();
        detailsMap.forEach((table_name, v) -> {
            collectTableResult.add(v);
        });
        return collectTableResult;
    }
}
