package hyren.serv6.t.dataReq;

import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.t.contants.ReqCategoryEnum;
import hyren.serv6.t.contants.ReqStatusEnum;
import hyren.serv6.t.entity.TskDataReq;
import hyren.serv6.t.entity.TskTblAssign;
import hyren.serv6.t.tableAssign.TskTableAssignServe;
import hyren.serv6.t.vo.query.TskDataReqQueryVo;
import hyren.serv6.t.vo.save.TskDataReqUpdateVo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.web.bind.annotation.*;
import javax.annotation.Resource;
import javax.validation.Valid;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api("数据需求信息表管理")
@RestController
@RequestMapping("dataReq")
public class TskDataReqController {

    @Resource
    private TskDataReqService tskDataReqService;

    @Resource
    private TskTableAssignServe tskTableAssignServe;

    @ApiOperation("列表")
    @GetMapping
    public Map<String, Object> queryByPage(@ApiParam(name = "tskDataReqQueryVo", value = "") TskDataReqQueryVo tskDataReqQueryVo, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<TskDataReqQueryVo> pageList = this.tskDataReqService.queryByPage(tskDataReqQueryVo, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    @ApiOperation("详情")
    @GetMapping("{id}")
    public TskDataReqQueryVo queryById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.tskDataReqService.queryById(id);
    }

    @ApiOperation("新增")
    @PostMapping
    public TskDataReq add(@ApiParam(name = "dataReqUpdateVo", value = "", required = true) @RequestBody TskDataReq tskDataReq) {
        return this.tskDataReqService.insert(tskDataReq);
    }

    @ApiOperation("编辑")
    @PutMapping
    public TskDataReq edit(@ApiParam(name = "dataReqUpdateVo", value = "", required = true) @Valid @RequestBody TskDataReqUpdateVo dataReqUpdateVo) {
        return this.tskDataReqService.update(dataReqUpdateVo);
    }

    @ApiOperation("删除")
    @DeleteMapping("{id}")
    public Boolean deleteById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.tskDataReqService.deleteById(id);
    }

    @ApiOperation("批量删除")
    @DeleteMapping("/batch")
    public Boolean batchDeleteByIds(@ApiParam(name = "ids", value = "", required = true) @RequestParam String ids) {
        return this.tskDataReqService.batchDeleteByIds(ids);
    }

    @ApiOperation("分配分配数据表")
    @PostMapping("/assign/tbl/{data_type}")
    public void assignTable(@ApiParam(name = "assignList", value = "", required = true) @Valid @RequestBody List<TskTblAssign> assignList, @ApiParam(name = "data_type", value = "", required = true) @PathVariable("data_type") String data_type) {
        this.tskDataReqService.relationTable(assignList, data_type);
    }

    @ApiOperation("获取分配数据表")
    @GetMapping("/assign/tbl")
    public List<TskTblAssign> getAssignTable(@ApiParam(name = "id", value = "", required = true) @RequestParam Long id) {
        return this.tskTableAssignServe.queryList(id, ReqCategoryEnum.DATA);
    }

    @ApiOperation("获取业务数据所分配的表")
    @GetMapping("/assign/treeTable")
    public List<Node> treeTable(Long id) {
        return tskDataReqService.getTreeTable(id, ReqCategoryEnum.BIZ);
    }

    @ApiOperation("变更数据需求状态")
    @PutMapping("status")
    public void changeStatus(@ApiParam(name = "id", value = "", required = true) @Valid @RequestParam Long id, @ApiParam(name = "status", value = "", required = true) @Valid @RequestParam String status) {
        this.tskDataReqService.changeStatus(id, ReqStatusEnum.ofEnumByCode(status));
    }
}
