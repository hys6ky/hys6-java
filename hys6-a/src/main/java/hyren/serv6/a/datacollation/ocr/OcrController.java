package hyren.serv6.a.datacollation.ocr;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Resource;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController("ocrController")
@RequestMapping("ocrRunBatch")
public class OcrController {

    @Resource(name = "ocrService")
    private OcrService ocrService;

    @ApiOperation(value = "", notes = "")
    @PostMapping("/getFileCollectionDataSources")
    public List<Map<String, Object>> getFileCollectionDataSources() {
        return ocrService.getFileCollectionDataSources();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "sourceId", value = "", example = "", dataTypeClass = String.class)
    @PostMapping("/getFileCollectionTasks")
    public List<Map<String, Object>> getFileCollectionTasks(@NotNull long sourceId) {
        return ocrService.getFileCollectionTasks(sourceId);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "fcs_id", value = "", example = "", dataTypeClass = long.class)
    @PostMapping("/startOcrRunBatch")
    public void startOcrRunBatch(long fcs_id) {
        ocrService.startOcrRunBatch(fcs_id);
    }
}
