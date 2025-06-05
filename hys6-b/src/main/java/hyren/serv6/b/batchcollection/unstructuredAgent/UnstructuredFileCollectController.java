package hyren.serv6.b.batchcollection.unstructuredAgent;

import fd.ng.db.resultset.Result;
import hyren.serv6.base.entity.FileCollectSet;
import hyren.serv6.base.entity.FileSource;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
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
@RequestMapping("/dataCollectionO/unstructuredAgent")
@RestController
@Api(value = "", tags = "")
@Validated
public class UnstructuredFileCollectController {

    @Autowired
    public UnstructuredFileCollectService unService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/searchFileCollect")
    @ApiImplicitParam(name = "file_collect_set", value = "", dataTypeClass = FileCollectSet.class)
    public Map<String, Object> searchFileCollect(@RequestBody FileCollectSet file_collect_set) {
        return unService.searchFileCollect(file_collect_set);
    }

    @RequestMapping("/searchFileCollectByAgent")
    public Map<String, Object> searchFileCollectByAgent(@NotNull Long agent_id) {
        return unService.searchFileCollectByAgent(agent_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/addFileCollect")
    @ApiImplicitParam(name = "file_collect_set", value = "", dataTypeClass = FileCollectSet.class)
    public long addFileCollect(FileCollectSet file_collect_set) {
        return unService.addFileCollect(file_collect_set);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/updateFileCollect")
    @ApiImplicitParam(name = "file_collect_set", value = "", dataTypeClass = FileCollectSet.class)
    public void updateFileCollect(FileCollectSet file_collect_set) {
        unService.updateFileCollect(file_collect_set);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/searchFileSource")
    @ApiImplicitParam(name = "fcs_id", value = "", dataTypeClass = Long.class)
    public Result searchFileSource(@NotNull Long fcs_id) {
        return unService.searchFileSource(fcs_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/selectPath")
    @ApiImplicitParams({ @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "path", value = "", dataTypeClass = String.class) })
    public List<Map> selectPath(@NotNull Long agent_id, String path) {
        return unService.selectPath(agent_id, path);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveFileSource")
    @ApiImplicitParam(name = "file_sources_array", value = "", dataTypeClass = String.class)
    public void saveFileSource(@RequestBody List<FileSource> fileSources) {
        unService.saveFileSource(fileSources);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/executeJob")
    @ApiImplicitParams({ @ApiImplicitParam(name = "fcs_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "execute_type", value = "", dataTypeClass = String.class) })
    public void executeJob(@NotNull Long fcs_id, String execute_type) {
        unService.executeJob(fcs_id, execute_type);
    }
}
