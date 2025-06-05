package hyren.serv6.h.market.moduletablefield;

import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@RequestMapping("/market/moduleTableField")
@RestController
@Slf4j
@Api
@Validated
public class DataTableFieldController {

    @Autowired
    DataTableFieldService dataTableFieldService;

    @RequestMapping("/findModuleTableFieldInfos")
    public List<Map<String, Object>> findModuleTableFieldInfos(@NotNull String dataTableId) {
        return dataTableFieldService.findModuleTableFieldInfos(dataTableId);
    }
}
