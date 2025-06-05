package hyren.serv6.m.metaData.formal;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.meta.TableMeta;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.m.contants.MetaObjTypeEnum;
import hyren.serv6.m.contants.MetaOperateMsgConstants;
import hyren.serv6.m.contants.TemplateConstants;
import hyren.serv6.m.entity.MetaObjInfo;
import hyren.serv6.m.log.MetaOperateLogService;
import hyren.serv6.m.main.metaUtil.MetaOperatorCustomize;
import hyren.serv6.m.util.FileDownLoadUtil;
import hyren.serv6.m.util.ResourceUtil;
import hyren.serv6.m.util.dbConf.ConnectionTool;
import hyren.serv6.m.vo.query.MetaObjInfoQueryVo;
import hyren.serv6.m.vo.save.MetaObjInfoSaveVo;
import hyren.serv6.m.vo.save.MetaObjTblColSaveVo;
import javassist.tools.reflect.Metaobject;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.multipart.MultipartFile;
import javax.annotation.Resource;
import javax.validation.Valid;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Api("元数据对象基础信息管理")
@RestController
@RequestMapping("metaObjInfo")
public class MetaObjInfoController {

    @Resource
    private MetaObjInfoService metaObjInfoService;

    @Resource
    private MetaOperateLogService operateLogService;

    @Resource
    private MetaCollTask metaCollTask;

    @ApiOperation("元数据对象基础信息列表")
    @GetMapping
    public Map<String, Object> queryByPage(@ApiParam(name = "MetaObjInfo", value = "") MetaObjInfoQueryVo metaObjInfoQueryVo, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<MetaObjInfoQueryVo> pageList = this.metaObjInfoService.queryByPage(metaObjInfoQueryVo, page);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    @ApiOperation("元数据对象基础信息详情")
    @GetMapping("{id}")
    public MetaObjInfoQueryVo queryById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.metaObjInfoService.queryById(id);
    }

    @ApiOperation("获取表中字段信息")
    @GetMapping("/getDataNum")
    public Long getColNum(@ApiParam(name = "id", value = "", required = true) Long id, @ApiParam(name = "tabName", value = "", required = true) String tabName) {
        return this.metaObjInfoService.getDataNum(id, tabName);
    }

    @ApiOperation("元数据对象基础信息新增")
    @PostMapping
    public MetaObjInfo add(@ApiParam(name = "MetaObjInfo", value = "", required = true) @RequestBody MetaObjInfoSaveVo metaObjInfoSaveVo) {
        return this.metaObjInfoService.insert(metaObjInfoSaveVo);
    }

    @ApiOperation("元数据对象基础信息编辑/批量编辑-只针对表")
    @PutMapping
    public MetaObjInfo edit(@ApiParam(name = "MetaObjInfoSaveVo", value = "", required = true) @Validated(MetaObjInfoSaveVo.Edit.class) @RequestBody MetaObjInfoSaveVo metaObjInfoSaveVo) {
        this.metaObjInfoService.update(metaObjInfoSaveVo);
        operateLogService.save(MetaOperateMsgConstants.OI_EDIT);
        return null;
    }

    @ApiOperation("修改表对象单个字段中文名")
    @PutMapping("/col")
    public void editColInfo(@ApiParam(name = "MetaObjTblColSaveVo", value = "", required = true) @Valid @RequestBody MetaObjTblColSaveVo tblColSaveVo) {
        this.metaObjInfoService.editColInfo(tblColSaveVo);
        operateLogService.save(MetaOperateMsgConstants.OI_COL_EDIT);
    }

    @ApiOperation("元数据对象基础信息删除")
    @DeleteMapping("{id}")
    public Boolean deleteById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.metaObjInfoService.deleteById(id);
    }

    @ApiOperation("下载元数据导入模板")
    @GetMapping("/download/template")
    public void downloadTemplate() {
        FileDownLoadUtil.exportToBrowser(ResourceUtil.getResourceAsStream(TemplateConstants.TMPL_PATH + TemplateConstants.TMPL_METADATA), TemplateConstants.TMPL_METADATA);
    }

    @ApiOperation("元数据版本比对")
    @GetMapping("/version/compare")
    public List<MetaObjInfoQueryVo> versionCompare(@ApiParam(name = "obj_id", value = "", required = true) Long obj_id, @ApiParam(name = "versions", value = "", required = true) String versions) {
        return metaObjInfoService.versionCompare(obj_id, versions);
    }

    @ApiOperation("获取元数据对象")
    @GetMapping("/getMetaData")
    public List<MetaObjInfo> getMetaData(@ApiParam(name = "dslId", value = "", required = true) Long dslId, @ApiParam(name = "objType", value = "", required = true) String objType, @ApiParam(name = "objName", value = "", required = true) String objName) {
        return metaObjInfoService.getMetaData(dslId, objType, objName);
    }

    @ApiOperation("元数据导入")
    @PostMapping("/import")
    public void metadataImport(@ApiParam(name = "source_id", value = "", required = true) Long source_id, @ApiParam(name = "file", value = "", required = true) MultipartFile file) {
        metaObjInfoService.metadataImport(source_id, file);
    }

    @ApiOperation("元数据导出")
    @GetMapping("/export")
    public void metedataExport(@ApiParam(name = "source_id", value = "", required = true) Long source_id) {
        metaObjInfoService.metaDateExport(source_id);
    }

    @ApiOperation("元数据模板下载")
    @GetMapping("/download")
    public void meteExcelDownload() {
        FileDownLoadUtil.exportToBrowser(ResourceUtil.getResourceAsStream(TemplateConstants.TMPL_PATH + TemplateConstants.TMPL_METADATA), TemplateConstants.TMPL_METADATA);
    }

    @ApiOperation("立即执行")
    @GetMapping("/run")
    public void isRun(@ApiParam(name = "task_id", value = "", required = true) Long task_id) {
        metaCollTask.runTask(task_id);
    }
}
