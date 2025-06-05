package hyren.serv6.m.metaData.his;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.m.entity.MetaObjInfoHis;
import hyren.serv6.m.vo.query.MetaObjInfoHisQueryVo;
import hyren.serv6.m.vo.save.MetaObjInfoHisSaveVo;
import org.springframework.web.bind.annotation.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiOperation;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api("对象基础信息历史管理")
@RestController
@RequestMapping("metaObjInfo/metaObjInfoHis")
public class MetaObjInfoHisController {

    @Resource
    private MetaObjInfoHisService metaObjInfoHisService;

    @ApiOperation("对象基础信息历史列表")
    @GetMapping
    public Map<String, Object> queryByPage(@ApiParam(name = "MetaObjInfoHis", value = "") MetaObjInfoHisQueryVo metaObjInfoHisQueryVo, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<MetaObjInfoHisQueryVo> pageList = this.metaObjInfoHisService.queryByPage(metaObjInfoHisQueryVo, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    @ApiOperation("对象基础信息历史详情")
    @GetMapping("{id}")
    public MetaObjInfoHisQueryVo queryById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long his_id) {
        return this.metaObjInfoHisService.queryByObjId(his_id);
    }

    @ApiOperation("对象基础信息历史新增")
    @PostMapping
    public MetaObjInfoHis add(@ApiParam(name = "MetaObjInfoHis", value = "", required = true) @RequestBody MetaObjInfoHisSaveVo metaObjInfoHisSaveVo) {
        return this.metaObjInfoHisService.insert(metaObjInfoHisSaveVo);
    }

    @ApiOperation("对象基础信息历史删除")
    @DeleteMapping("{id}")
    public Boolean deleteById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.metaObjInfoHisService.deleteById(id);
    }
}
