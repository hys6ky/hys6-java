package hyren.serv6.m.dataSource;

import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.serv6.m.contants.MetaObjTypeEnum;
import hyren.serv6.m.contants.MetaOperateMsgConstants;
import hyren.serv6.m.contants.MetadataSourceEnum;
import hyren.serv6.m.entity.MetaDataSource;
import hyren.serv6.m.log.MetaOperateLogService;
import hyren.serv6.m.task.MetaTaskService;
import hyren.serv6.m.vo.query.MetaDataSourceQueryVo;
import hyren.serv6.m.vo.save.MetaDataSourceSaveVo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.web.bind.annotation.*;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api("数据源信息管理")
@RestController
@RequestMapping("/metaTask/metaDataSource")
public class MetaDataSourceController {

    @Resource
    private MetaDataSourceService metaDataSourceService;

    @Resource
    private MetaTaskService metaTaskService;

    @Resource
    private MetaOperateLogService operateLogService;

    @ApiOperation("数据源信息列表")
    @GetMapping
    public Map<String, Object> queryByPage(@ApiParam(name = "metaDataSourceQueryVo", value = "") MetaDataSourceQueryVo metaDataSourceQueryVo, @ApiParam(name = "currPage", value = "", required = true) int currPage, @ApiParam(name = "pageSize", value = "", required = true) int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<MetaDataSourceQueryVo> pageList = this.metaDataSourceService.queryByPage(metaDataSourceQueryVo, page);
        if (metaDataSourceQueryVo.getShowTask()) {
            for (MetaDataSourceQueryVo dataSourceQueryVo : pageList) {
                dataSourceQueryVo.setTblTaskQueryVoList(metaTaskService.queryBySourceIdAndTaskType(dataSourceQueryVo.getSource_id(), MetaObjTypeEnum.TBL));
                dataSourceQueryVo.setViewTaskQueryVoList(metaTaskService.queryBySourceIdAndTaskType(dataSourceQueryVo.getSource_id(), MetaObjTypeEnum.VIEW));
                dataSourceQueryVo.setMeterViewTaskQueryVoList(metaTaskService.queryBySourceIdAndTaskType(dataSourceQueryVo.getSource_id(), MetaObjTypeEnum.METER_VIEW));
                dataSourceQueryVo.setProcTaskQueryVoList(metaTaskService.queryBySourceIdAndTaskType(dataSourceQueryVo.getSource_id(), MetaObjTypeEnum.PROC));
                if (dataSourceQueryVo.getDsl_id() != null) {
                    dataSourceQueryVo.setDslName(metaDataSourceService.getDslName(dataSourceQueryVo.getDsl_id()));
                }
            }
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("totalSize", page.getTotalSize());
        resultMap.put("pageList", pageList);
        return resultMap;
    }

    @ApiOperation("数据源信息详情")
    @GetMapping("{id}")
    public MetaDataSourceQueryVo queryById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        return this.metaDataSourceService.queryById(id);
    }

    @ApiOperation("数据源信息新增")
    @PostMapping
    public MetaDataSource add(@ApiParam(name = "MetaDataSource", value = "", required = true) @RequestBody MetaDataSourceSaveVo metaDataSourceSaveVo) {
        if (MetadataSourceEnum.DB.getCode().equals(metaDataSourceSaveVo.getDs_type()) && null == metaDataSourceSaveVo.getDsl_id()) {
            throw new SystemBusinessException("选择存储层时，存储层id必传");
        }
        MetaDataSource dataSource = this.metaDataSourceService.insert(metaDataSourceSaveVo);
        operateLogService.save(MetaOperateMsgConstants.DS_ADD);
        return dataSource;
    }

    @ApiOperation("批量数据源信息新增")
    @PostMapping("/batchAdd")
    public void batchAdd(@ApiParam(name = "MetaDataSource", value = "", required = true) @RequestBody List<MetaDataSourceSaveVo> metaDataSourceSaveVos) {
        metaDataSourceSaveVos.forEach(metaDataSourceSaveVo -> {
            if (MetadataSourceEnum.DB.getCode().equals(metaDataSourceSaveVo.getDs_type()) && null == metaDataSourceSaveVo.getDsl_id()) {
                throw new SystemBusinessException("选择存储层时，存储层id必传");
            }
            MetaDataSource dataSource = this.metaDataSourceService.insert(metaDataSourceSaveVo);
            operateLogService.save(MetaOperateMsgConstants.DS_ADD);
        });
    }

    @ApiOperation("数据源信息编辑")
    @PutMapping
    public MetaDataSource edit(@ApiParam(name = "MetaDataSource", value = "", required = true) @RequestBody MetaDataSourceSaveVo metaDataSourceSaveVo) {
        MetaDataSource dataSource = this.metaDataSourceService.update(metaDataSourceSaveVo);
        operateLogService.save(MetaOperateMsgConstants.DS_EDIT);
        return dataSource;
    }

    @ApiOperation("数据源信息删除")
    @DeleteMapping("{id}")
    public Boolean deleteById(@ApiParam(name = "id", value = "", required = true) @PathVariable("id") Long id) {
        this.metaDataSourceService.deleteById(id);
        operateLogService.save(MetaOperateMsgConstants.DS_DEL);
        return null;
    }

    @ApiOperation("获取系统配置信息")
    @GetMapping("/getSysPara")
    public Map<String, String> getSysPara(@ApiParam(name = "paraType", value = "", required = true) String paraType) {
        Validator.notNull(paraType);
        return metaDataSourceService.getSysPara(paraType);
    }
}
