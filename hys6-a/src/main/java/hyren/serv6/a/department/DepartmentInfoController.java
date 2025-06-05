package hyren.serv6.a.department;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.a.node.TreeNode;
import hyren.serv6.base.entity.DepartmentInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/departmentalList")
@Api(tags = "")
public class DepartmentInfoController {

    @Autowired
    private DepartmentService departmentService;

    @ApiOperation("分页查询")
    @PostMapping("/getDataByPage")
    @ApiImplicitParam(name = "departmentInfoVo", value = "", paramType = "query")
    public Map<String, Object> getDataByPage(Long dep_id, Integer currPage, Integer pageSize) {
        DepartmentInfo departmentInfo = new DepartmentInfo();
        if (dep_id != null) {
            departmentInfo.setDep_id(dep_id);
        }
        Page page = new DefaultPageImpl(currPage, pageSize);
        return departmentService.getDataByPage(departmentInfo, page);
    }

    @ApiOperation("根据Id获取机构信息")
    @PostMapping("/getByDepId")
    @ApiImplicitParam(name = "dep_id", value = "", paramType = "query", dataType = "Long", required = true)
    public DepartmentInfo getByDepId(Long dep_id) {
        return departmentService.getByDepId(dep_id);
    }

    @ApiOperation("新增机构信息")
    @PostMapping("/add")
    @ApiImplicitParam(name = "departmentInfo", value = "", paramType = "body", dataType = "DepartmentInfo", required = true)
    public DepartmentInfo add(@RequestBody @Validated DepartmentInfo departmentInfo) {
        return departmentService.add(departmentInfo);
    }

    @ApiOperation("更新机构信息")
    @PostMapping("/update")
    @ApiImplicitParam(name = "departmentInfo", value = "", paramType = "body", dataType = "DepartmentInfo", required = true)
    public DepartmentInfo update(@RequestBody @Validated DepartmentInfo departmentInfo) {
        return departmentService.update(departmentInfo);
    }

    @ApiOperation("删除机构信息")
    @PostMapping("/deleteById")
    @ApiImplicitParam(name = "dep_id", value = "", paramType = "query", dataType = "Long", required = true)
    public boolean deleteById(Long dep_id) {
        return departmentService.deleteById(dep_id);
    }

    @ApiOperation("获取度量列表信息")
    @PostMapping("/getAllTree")
    public List<TreeNode> getAllTree() {
        return departmentService.getAllTree();
    }
}
