package hyren.serv6.a.bigscreendisplay;

import fd.ng.db.resultset.Result;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController("bigScreenDisplayController")
@RequestMapping("/bigscreendisplay")
public class BigScreenDisplayController {

    @Autowired
    private BigScreenDisplayService bigScreenDisplayService;

    @ApiOperation(value = "", notes = "")
    @RequestMapping("/failuresJobNum")
    public long failuresJobNum() {
        return bigScreenDisplayService.querylong();
    }

    @ApiOperation(value = "")
    @ApiImplicitParam(name = "yearData", value = "", example = "", dataTypeClass = String.class)
    @RequestMapping("/totalSystemCapacity")
    public String totalSystemCapacity(String yearData) {
        return bigScreenDisplayService.queryString(yearData);
    }

    @ApiOperation(value = "")
    @ApiImplicitParam(name = "yearData", value = "", example = "", dataTypeClass = String.class)
    @RequestMapping("/totalNumberOfData")
    public String totalNumberOfData(String yearData) {
        return bigScreenDisplayService.totalNumberOfData(yearData);
    }

    @ApiOperation(value = "")
    @ApiImplicitParam(name = "yearData", value = "", example = "", dataTypeClass = String.class)
    @RequestMapping("/totalNumberOfConnectedSystems")
    public long totalNumberOfConnectedSystems(String yearData) {
        return bigScreenDisplayService.queryTotalLong(yearData);
    }

    @ApiOperation(value = "")
    @RequestMapping("/totalNumberOfAccessDatabases")
    public long totalNumberOfAccessDatabases() {
        return bigScreenDisplayService.querytotalNumber();
    }

    @ApiOperation(value = "")
    @ApiImplicitParam(name = "yearData", value = "", example = "", dataTypeClass = String.class)
    @RequestMapping("/totalNumberOfAccessDataTables")
    public Long totalNumberOfAccessDataTables(String yearData) {
        return bigScreenDisplayService.totalNumberOfAccessDataTables(yearData);
    }

    @ApiOperation(value = "")
    @RequestMapping("/totalNumberOfCollect")
    public Map<String, Object> totalNumberOfCollect() {
        return bigScreenDisplayService.queryTotalNumberOfCollect();
    }

    @ApiOperation(value = "")
    @RequestMapping("/numberOfTableCollectedToday")
    public long numberOfTableCollectedToday() {
        return bigScreenDisplayService.queryNumberOfTableCollectedToday();
    }

    @ApiOperation(value = "")
    @RequestMapping("/numberOfFailedCollectionTablesToday")
    public long numberOfFailedCollectionTablesToday() {
        return bigScreenDisplayService.numberOfFailedCollectionTablesToday();
    }

    @ApiOperation(value = "")
    @RequestMapping("/newAccessData")
    public long newAccessData() {
        return bigScreenDisplayService.newAccessData();
    }

    @ApiOperation(value = "")
    @RequestMapping("/dataProcessingVolume")
    public long dataProcessingVolume() {
        return bigScreenDisplayService.dataProcessingVolume();
    }

    @ApiOperation(value = "")
    @RequestMapping("/numberOfTablesAfterProcessing")
    public long numberOfTablesAfterProcessing() {
        return bigScreenDisplayService.numberOfTablesAfterProcessing();
    }

    @ApiOperation(value = "")
    @RequestMapping("/dataSchedulingSituation")
    public List<Map<String, Object>> dataSchedulingSituation() {
        return bigScreenDisplayService.dataSchedulingSituation();
    }

    @ApiOperation(value = "", notes = "")
    @RequestMapping("/interfaceCallSituation")
    public List<Map<String, Object>> interfaceCallSituation() {
        return bigScreenDisplayService.interfaceCallSituation();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "startTime", value = "", example = "", defaultValue = "0", dataTypeClass = long.class), @ApiImplicitParam(name = "endTime", value = "", example = "", defaultValue = "0", dataTypeClass = long.class) })
    @RequestMapping("/getResourceInfo")
    public List<Map<String, Object>> getResourceInfo(long startTime, long endTime) {
        return bigScreenDisplayService.getResourceInfo(startTime, endTime);
    }

    @ApiOperation(value = "", notes = "")
    @RequestMapping("/collectTotalSize")
    public long collectTotalSize() {
        return bigScreenDisplayService.collectTotalSize();
    }

    @ApiOperation(value = "", notes = "")
    @RequestMapping("/datasourceAndCollectTotalSize")
    public Result datasourceAndCollectTotalSize() {
        return bigScreenDisplayService.datasourceAndCollectTotalSize();
    }

    @ApiOperation(value = "")
    @RequestMapping("/numberOfInterfaceToday")
    public long numberOfInterfaceToday() {
        return bigScreenDisplayService.numberOfInterfaceToday();
    }

    @ApiOperation(value = "")
    @RequestMapping("/newAccessDatabaseYear")
    public long newAccessDatabaseYear() {
        return bigScreenDisplayService.newAccessDatabaseYear();
    }

    @ApiOperation(value = "")
    @RequestMapping("/numberOfFileToday")
    public long numberOfFileToday() {
        return bigScreenDisplayService.numberOfFileToday();
    }

    @ApiOperation(value = "")
    @RequestMapping("/numberOfFileYear")
    public long numberOfFileYear() {
        return bigScreenDisplayService.numberOfFileYear();
    }
}
