package hyren.serv6.h.market.dmjobtable;

import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import hyren.serv6.base.entity.DmJobTableInfo;
import hyren.serv6.base.exception.BusinessException;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@RequestMapping("/market/dmJobTableInfo")
@RestController
@Api
@Validated
@Slf4j
public class DmJobTableInfoController {

    @Autowired
    DmJobTableInfoService dmJobTableInfoService;

    @RequestMapping("/findDmJobTableInfoByTaskId")
    public List<Map<String, Object>> findDmJobTableInfoByTaskId(String taskId) {
        return dmJobTableInfoService.findDmJobTableInfoByTaskId(taskId);
    }

    @RequestMapping("/getDataBySQL")
    public List<Map<String, Object>> getDataBySQL(@RequestParam String querysql, @RequestParam String sqlparameter) {
        Validator.notBlank(querysql, "查询sql不能为空");
        return dmJobTableInfoService.getDataBySQL(querysql, sqlparameter);
    }

    @RequestMapping("/getColumnBySql")
    public Map<String, Object> getColumnBySql(@RequestParam String querysql, @RequestParam String datatable_id, @RequestParam String sqlparameter) {
        return dmJobTableInfoService.getColumnBySql(querysql, datatable_id, sqlparameter);
    }

    @RequestMapping("/findJobs")
    public List<DmJobTableInfo> findJobs(String taskId) {
        return dmJobTableInfoService.findJobs(taskId);
    }

    @RequestMapping("/delByJobTableId")
    public boolean delByJobTableId(Long jobTableId) {
        return dmJobTableInfoService.delJobTableByJobTableId(jobTableId);
    }

    @RequestMapping("/delJobFieldByJobTableId")
    public boolean delJobFieldByJobTableId(Long jobTableId) {
        return dmJobTableInfoService.delJobFieldByJobTableId(jobTableId);
    }

    @RequestMapping("/checkTableName")
    public boolean checkTableName(String tableName, String moduleTableId) {
        return dmJobTableInfoService.checkTableName(tableName, moduleTableId);
    }

    @RequestMapping("/findDmTaskDataTableByTaskId")
    public DmJobTableInfo findDmTaskDataTableByTaskId(String job_table_id) {
        return dmJobTableInfoService.findDmTaskDataTableByTaskId(job_table_id);
    }
}
