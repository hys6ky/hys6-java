package hyren.serv6.m.metaDataCheck;

import fd.ng.core.utils.Validator;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("metaTask/metaDataCheck")
public class MetaDataCheckController {

    private MetaDataCheckServe metaDataCheckServe;

    public MetaDataCheckController(MetaDataCheckServe metaDataCheckServe) {
        this.metaDataCheckServe = metaDataCheckServe;
    }

    @ApiOperation("获取源数据检核")
    @GetMapping("/getTable")
    public List<Map<String, Object>> getTable(Long source_id) {
        Validator.notNull(source_id, "元系统Id不能为空");
        return metaDataCheckServe.getSourceTable(source_id);
    }
}
