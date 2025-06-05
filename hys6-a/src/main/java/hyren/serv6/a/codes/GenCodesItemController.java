package hyren.serv6.a.codes;

import fd.ng.db.resultset.Result;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@RestController
@RequestMapping("/codes")
@Api(tags = "")
@Validated
public class GenCodesItemController {

    @Autowired
    GenCodesItemService genCodesItemService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "category", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "code", value = "", dataTypeClass = String.class, example = "") })
    @RequestMapping("/getValue")
    public String getValue(String category, String code) {
        return genCodesItemService.getValue(category, code);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "category", value = "", dataTypeClass = String.class, example = "")
    @RequestMapping("/getCategoryItems")
    public Result getCategoryItems(String category) {
        return genCodesItemService.getCategoryItems(category);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "category", value = "", dataTypeClass = String.class, example = "")
    @RequestMapping("/getCodeItems")
    public Map<String, String> getCodeItems(String category) {
        return genCodesItemService.getCodeItems(category);
    }

    @ApiOperation(value = "", notes = "")
    @RequestMapping("/getAllCodeItems")
    public Map<String, Object> getAllCodeItems() {
        return genCodesItemService.getAllCodeItems();
    }
}
