package hyren.serv6.h.market.dmtaskinfo;

import hyren.serv6.base.entity.DmTaskInfo;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Slf4j
@RequestMapping("/market/dmTaskInfo")
@RestController
@Validated
@Api("dm_task_info 基础类")
public class DmTaskInfoController {

    @Autowired
    DmTaskInfoService dmTaskInfoService;

    @RequestMapping("/addDmTaskInfo")
    public boolean addDmTaskInfo(@RequestBody DmTaskInfo dmTaskInfo) {
        return dmTaskInfoService.addDmTaskInfo(dmTaskInfo);
    }

    @RequestMapping("/addDmTaskInfos")
    public boolean addDmTaskInfos(@RequestBody List<DmTaskInfo> dmTaskInfos) {
        return dmTaskInfoService.addDmTaskInfos(dmTaskInfos);
    }

    @RequestMapping("/delDmTaskInfo")
    public boolean delDmTaskInfo(@NotNull Long dmTaskInfoId) {
        return dmTaskInfoService.delDmTaskInfo(dmTaskInfoId);
    }

    @RequestMapping("/updateDmTaskInfo")
    public boolean updateDmTaskInfo(@RequestBody DmTaskInfo dmTaskInfo) {
        return dmTaskInfoService.updateDmTaskInfo(dmTaskInfo);
    }

    @RequestMapping("/findDmTaskInfos")
    public List<DmTaskInfo> findDmTaskInfos() {
        return dmTaskInfoService.findDmTaskInfos();
    }

    @RequestMapping("/findDmTaskInfoById")
    public Map<String, Object> findDmTaskInfoById(@NotNull Long dmTaskInfoId) {
        return dmTaskInfoService.findDmTaskInfoById(dmTaskInfoId);
    }

    @RequestMapping("/findDmTaskInfosByTableId")
    public List<DmTaskInfo> findDmTaskInfosByTableId(@NotNull Long dataTableId) {
        return dmTaskInfoService.findDmTaskInfosByTableId(dataTableId);
    }
}
