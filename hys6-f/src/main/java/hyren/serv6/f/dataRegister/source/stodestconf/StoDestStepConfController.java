package hyren.serv6.f.dataRegister.source.stodestconf;

import fd.ng.core.annotation.Return;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.exception.BusinessException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Objects;

@RequestMapping("/dataRegister/agent/stodestconf")
@RestController
@Slf4j
@Api("定义存储目的地配置")
@Validated
public class StoDestStepConfController {

    @Autowired
    StoDestStepService stoDestStepConfService;

    @ApiOperation(value = "", tags = "")
    @Return(desc = "", range = "")
    @RequestMapping("/getStorageData")
    public Result getStorageData() {
        return stoDestStepConfService.getStorageData();
    }

    @ApiOperation(value = "", tags = "")
    @Return(desc = "", range = "")
    @RequestMapping("/getStorageDataBySource")
    public Result getStorageDataBySource() {
        return stoDestStepConfService.getStorageDataBySource();
    }

    @ApiOperation(value = "", tags = "")
    @ApiImplicitParam(name = "dslId", value = "", type = "不为空", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getStoDestDetail")
    public String getStoDestDetail(Long dslId) {
        if (Objects.isNull(dslId)) {
            throw new BusinessException("please check one database.");
        }
        return stoDestStepConfService.getStoDestDetail(dslId);
    }
}
