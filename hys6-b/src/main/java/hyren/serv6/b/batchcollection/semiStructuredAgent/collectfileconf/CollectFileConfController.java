package hyren.serv6.b.batchcollection.semiStructuredAgent.collectfileconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.entity.ObjectCollectStruct;
import hyren.serv6.base.entity.ObjectCollectTask;
import hyren.serv6.base.entity.ObjectHandleType;
import hyren.serv6.base.exception.BusinessException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@RestController
@Slf4j
@RequestMapping("/dataCollectionO/semiStructuredAgent/collectfileconf")
@Api(value = "", tags = "")
@Validated
public class CollectFileConfController {

    @Autowired
    public CollectFileConfService confService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/searchObjectCollectTask")
    @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class)
    public Map<String, Object> searchObjectCollectTask(@NotNull Long odc_id) {
        return confService.searchObjectCollectTask(odc_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getObjectCollectStruct")
    @ApiImplicitParams({ @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "ocs_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "en_name", value = "", dataTypeClass = String.class) })
    public List<ObjectCollectStruct> getObjectCollectStruct(@NotNull Long odc_id, @NotNull Long ocs_id, String en_name) {
        return confService.getObjectCollectStruct(odc_id, ocs_id, en_name);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getFirstLineTreeInfo")
    @ApiImplicitParams({ @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "ocs_id", value = "", dataTypeClass = Long.class) })
    public List<Map<String, Object>> getFirstLineTreeInfo(@NotNull Long odc_id, @NotNull Long ocs_id) {
        return confService.getFirstLineTreeInfo(odc_id, ocs_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getObjectCollectStructById")
    @ApiImplicitParam(name = "ocs_id", value = "", dataTypeClass = Long.class)
    public List<ObjectCollectStruct> getObjectCollectStructById(@NotNull Long ocs_id) {
        return confService.getObjectCollectStructById(ocs_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/searchObjectHandleType")
    @ApiImplicitParams({ @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "en_name", value = "", dataTypeClass = String.class) })
    public List<ObjectHandleType> searchObjectHandleType(@NotNull Long odc_id, String en_name) {
        return confService.searchObjectHandleType(odc_id, en_name);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveObjectHandleType")
    @ApiImplicitParams({ @ApiImplicitParam(name = "ocs_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "objectHandleTypes", value = "", dataTypeClass = ObjectHandleType[].class) })
    public void saveObjectHandleType(@RequestBody Map<String, Object> req) {
        long ocs_id = 0;
        String objectHandleTypes = null;
        try {
            ocs_id = Long.parseLong(req.get("ocs_id").toString());
            objectHandleTypes = Objects.isNull(req.get("objectHandleTypes")) ? StringUtils.EMPTY : req.get("objectHandleTypes").toString();
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("req format failed.");
        }
        ObjectHandleType[] result = JsonUtil.toObject(objectHandleTypes, new TypeReference<ObjectHandleType[]>() {
        });
        confService.saveObjectHandleType(ocs_id, result);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getObjectHandleType")
    @ApiImplicitParam(name = "ocs_id", value = "", dataTypeClass = Long.class)
    public Result getObjectHandleType(@NotNull Long ocs_id) {
        return confService.getObjectHandleType(ocs_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getObjectCollectTreeInfo")
    @ApiImplicitParams({ @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "ocs_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "location", value = "", dataTypeClass = String.class) })
    public List<Map<String, Object>> getObjectCollectTreeInfo(@NotNull Long odc_id, @NotNull Long ocs_id, String location) {
        return confService.getObjectCollectTreeInfo(odc_id, ocs_id, location);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveObjectCollectStruct")
    @ApiImplicitParams({ @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "ocs_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "objectCollectStructs", value = "", dataTypeClass = ObjectCollectStruct[].class) })
    public void saveObjectCollectStruct(@RequestBody Map<String, Object> req) {
        long odc_id = 0;
        long ocs_id = 0;
        String objectCollectStructs = null;
        try {
            odc_id = Long.parseLong(req.get("odc_id").toString());
            ocs_id = Long.parseLong(req.get("ocs_id").toString());
            objectCollectStructs = req.get("objectCollectStructs").toString();
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("req format failed.");
        }
        ObjectCollectStruct[] result = JsonUtil.toObject(objectCollectStructs, new TypeReference<ObjectCollectStruct[]>() {
        });
        confService.saveObjectCollectStruct(odc_id, ocs_id, result);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveObjectCollectTask")
    @ApiImplicitParams({ @ApiImplicitParam(name = "agent_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "objectCollectTasks", value = "", dataTypeClass = ObjectCollectTask[].class) })
    public void saveObjectCollectTask(@RequestBody Map<String, Object> req) {
        long agent_id = 0;
        long odc_id = 0;
        String objectCollectTasks = null;
        try {
            agent_id = Long.parseLong(req.get("agent_id").toString());
            odc_id = Long.parseLong(req.get("odc_id").toString());
            objectCollectTasks = Objects.isNull(req.get("objectCollectTasks")) ? StringUtils.EMPTY : req.get("objectCollectTasks").toString();
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("req format failed.");
        }
        ObjectCollectTask[] result = JsonUtil.toObject(objectCollectTasks, new TypeReference<ObjectCollectTask[]>() {
        });
        confService.saveObjectCollectTask(agent_id, odc_id, result);
    }
}
