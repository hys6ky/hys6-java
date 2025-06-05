package hyren.serv6.h.market.dmcategory;

import hyren.serv6.base.entity.DmCategory;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.List;

@RequestMapping("/market/dmCategory")
@RestController
@Validated
@Slf4j
@Api("分类基础类")
public class DmCategoryController {

    @Autowired
    DmCategoryService dmCategoryService;

    @RequestMapping("/addDmCategory")
    public boolean addDmCategory(@RequestBody DmCategory dmCategory) {
        return dmCategoryService.addDmCategory(dmCategory);
    }

    @RequestMapping("/delDmCategory")
    public boolean delDmCategory(@NotNull Long categoryId) {
        return dmCategoryService.delDmCategory(categoryId);
    }

    @RequestMapping("/updateDmCategory")
    public boolean updateDmCategory(@RequestBody DmCategory dmCategory) {
        return dmCategoryService.updateDmCategory(dmCategory);
    }

    @RequestMapping("/findDmCategorys")
    public List<DmCategory> findDmCategorys() {
        return dmCategoryService.findDmCategorys();
    }

    @RequestMapping("/findDmCategorysByDmInfoId")
    public List<DmCategory> findDmCategorysByDmInfoId(@NotNull Long data_mart_id) {
        return dmCategoryService.findDmCategorysByDmInfoId(data_mart_id);
    }

    @RequestMapping("/findDmCategoryById")
    public DmCategory findDmCategoryById(@NotNull Long categoryId) {
        return dmCategoryService.findDmCategoryById(categoryId);
    }
}
