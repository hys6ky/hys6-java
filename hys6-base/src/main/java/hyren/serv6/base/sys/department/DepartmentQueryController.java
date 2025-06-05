package hyren.serv6.base.sys.department;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.DepartmentInfo;
import hyren.serv6.base.exception.BusinessException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Api(tags = "")
@RestController
@RequestMapping("/departmentalList")
@Configuration
@Validated
public class DepartmentQueryController {

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", example = "", dataTypeClass = int.class), @ApiImplicitParam(name = "pageSize", value = "", example = "", dataTypeClass = int.class) })
    @PostMapping("/getDepartmentInfo")
    public Map<String, Object> getDepartmentInfoByPage(int currPage, int pageSize) {
        Map<String, Object> departmentInfoMap = new HashMap<>();
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DepartmentInfo> departmentInfos = Dbo.queryPagedList(DepartmentInfo.class, page, "select * from " + DepartmentInfo.TableName);
        departmentInfoMap.put("departmentInfos", departmentInfos);
        departmentInfoMap.put("totalSize", page.getTotalSize());
        return departmentInfoMap;
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dep_id", value = "", example = "", dataTypeClass = long.class)
    @PostMapping("/checkDepIdIsExist")
    public boolean checkDepIdIsExist(@NotNull(message = "") long dep_id) {
        return Dbo.queryNumber("SELECT COUNT(dep_id) FROM " + DepartmentInfo.TableName + " WHERE dep_id = ?", dep_id).orElseThrow(() -> new BusinessException("检查部门否存在的SQL编写错误")) == 1;
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", example = "", dataTypeClass = int.class), @ApiImplicitParam(name = "pageSize", value = "", example = "", dataTypeClass = int.class) })
    @PostMapping("/getAllDepartmentInfo")
    public List<DepartmentInfo> getAllDepartmentInfo() {
        return Dbo.queryList(DepartmentInfo.class, "select * from " + DepartmentInfo.TableName + " order by dep_id, create_date asc, create_time asc");
    }
}
