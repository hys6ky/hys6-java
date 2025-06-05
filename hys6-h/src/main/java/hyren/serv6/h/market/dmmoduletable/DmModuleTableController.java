package hyren.serv6.h.market.dmmoduletable;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.entity.DmModuleTable;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RequestMapping("/market/dmModuleTable")
@RestController
@Slf4j
@Api("dm_dataTable基础类")
@Validated
public class DmModuleTableController {

    @Autowired
    DmModuleTableService dmModuleTableService;

    @RequestMapping("/addDmModuleTable/{dslId}")
    public Long addDmModuleTable(@PathVariable long dslId, @RequestBody DmModuleTable dmDatatable) {
        return dmModuleTableService.addDmModuleTable(dslId, dmDatatable);
    }

    @RequestMapping("/delDmModuleTable")
    public boolean delDmModuleTable(@NotNull Long dmDatatableId) {
        return dmModuleTableService.delDmModuleTable(dmDatatableId);
    }

    @RequestMapping("/updateDmModuleTable/{dslId}")
    public boolean updateDmModuleTable(@PathVariable long dslId, @RequestBody DmModuleTable dmDatatable) {
        return dmModuleTableService.updateDmModuleTable(dslId, dmDatatable);
    }

    @RequestMapping("/findDmModuleTables")
    public List<DmModuleTable> findDmModuleTables() {
        return dmModuleTableService.findDmModuleTables();
    }

    @RequestMapping("/findDmModuleTablesByDmInfoIdAndCateId")
    public List<DmModuleTableDto> findDmModuleTablesByDmInfoIdAndCateId(Long dmInfoId, Long cateId) {
        return dmModuleTableService.findDmModuleTablesByDmInfoIdAndCateId(dmInfoId, cateId);
    }

    @RequestMapping("/findDmModuleTableById")
    public DmModuleTable findDmModuleTableById(@NotNull Long dmDatatableId) {
        return dmModuleTableService.findDmModuleTableById(dmDatatableId);
    }

    @RequestMapping("/queryDmModuleTableByDataTableId")
    public List<Map<String, Object>> queryDmModuleTableByDataTableId(@NotNull Long dmDatatableId) {
        return dmModuleTableService.queryDmModuleTableByDataTableId(dmDatatableId);
    }

    @RequestMapping("/findDataByDmTableName")
    public List<Map<String, Object>> findDataByDmTableName(@NotNull String dmTableName) {
        return dmModuleTableService.findDataByDmTableName(dmTableName);
    }

    @RequestMapping("/checkTableName")
    public boolean checkTableName(String tableName) {
        Optional<DmModuleTable> dmModuleTable = dmModuleTableService.checkTableName(tableName);
        return !dmModuleTable.isPresent();
    }

    @RequestMapping("/delDmModuleTableByIds")
    public boolean delDmModuleTableByIds(@RequestBody Map<String, Object> ids) {
        List<String> longs = JsonUtil.toObject(ids.get("ids").toString(), new TypeReference<List<String>>() {
        });
        return dmModuleTableService.delDmModuleTableByIds(longs);
    }

    @RequestMapping("/checkModuleTableFields")
    public boolean checkModuleTableFields(String moduleTableId) {
        return dmModuleTableService.checkModuleTableFields(moduleTableId);
    }

    @RequestMapping("/checkModuleTableIsRun")
    public boolean checkModuleTableIsRun(String moduleTableId) {
        return dmModuleTableService.checkModuleTableIsRun(moduleTableId);
    }
}
