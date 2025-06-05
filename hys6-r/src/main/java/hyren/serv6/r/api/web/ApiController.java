package hyren.serv6.r.api.web;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.MapUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.resultset.Result;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DFAppState;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.Status;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.r.api.service.ApiDfProInfoApiServiceImpl;
import hyren.serv6.r.api.service.ApiDfTableApplyServiceImpl;
import hyren.serv6.r.api.service.ApiServiceImpl;
import hyren.serv6.r.api.service.DataLayerServiceImpl;
import hyren.serv6.r.record.service.impl.DfTableApplyServiceImpl;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/dataSupplementApi")
@Api(tags = "")
@Validated
public class ApiController {

    @Autowired
    ApiServiceImpl apiServiceImpl;

    @Autowired
    ApiDfProInfoApiServiceImpl apiDfProInfoApiServiceImpl;

    @Autowired
    ApiDfTableApplyServiceImpl apiDfTableApplyServiceImpl;

    @Autowired
    DfTableApplyServiceImpl dfTableApplyServiceImpl;

    @Autowired
    DataLayerServiceImpl dataLayerServiceImpl;

    @ApiOperation(value = "")
    @PostMapping("/getApiList")
    public Map<String, Object> getApiList(Integer currPage, Integer pageSize) {
        String apiSql = "select t1.*,t2.dsl_name From (SELECT a.*, p.DSL_ID FROM " + DfApiDef.TableName + " a left JOIN " + DfTableApply.TableName + " t ON a.APPLY_TAB_ID = t.APPLY_TAB_ID left JOIN " + DfProInfo.TableName + " p ON t.DF_PID = p.DF_PID) t1 left join " + DataStoreLayer.TableName + " t2 on t1.dsl_id = t2.dsl_id";
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> dfApiList = Dbo.queryPagedList(page, apiSql);
        Map<String, Object> map = new HashMap<>();
        map.put("dfApiList", dfApiList);
        map.put("totalSize", page.getTotalSize());
        return map;
    }

    @ApiOperation(value = "")
    @PostMapping("/addApi")
    public void addApi(@RequestBody Map<String, Object> requestData) {
        Map<String, Object> dataFillObjectMap = (Map<String, Object>) requestData.get("dfApiDef");
        Long table_id = Long.parseLong(dataFillObjectMap.get("table_id").toString());
        Long df_pid = Long.parseLong(dataFillObjectMap.get("df_pid").toString());
        List<Map<String, Object>> apiAttrObjectList = new ArrayList<>();
        Object dfApiAttrObj = requestData.get("dfApiAttr");
        if (dfApiAttrObj instanceof List) {
            apiAttrObjectList = (List<Map<String, Object>>) dfApiAttrObj;
        }
        if (apiAttrObjectList.size() == 0) {
            throw new BusinessException("未选择可用字段,请选择后重新提交!");
        }
        DfApiDef dfApiDef = new DfApiDef();
        dfApiDef.setApi_id(PrimayKeyGener.getNextId());
        this.setDfApiDef(dataFillObjectMap, dfApiDef);
        List<DfApiAttr> listDfAttr = new ArrayList<>();
        for (Map<String, Object> apiAttrObjectMap : apiAttrObjectList) {
            DfApiAttr dfApiAttr = new DfApiAttr();
            dfApiAttr.setApi_id(dfApiDef.getApi_id());
            dfApiAttr.setDaa_id(PrimayKeyGener.getNextId());
            this.setDfApiAttr(apiAttrObjectMap, dfApiAttr);
            listDfAttr.add(dfApiAttr);
        }
        DfTableApply dfTableApply = apiDfTableApplyServiceImpl.getDfTableApplyById(table_id, df_pid);
        if (dfTableApply.getApply_tab_id() == null) {
            Map<String, Object> resultMap = apiDfProInfoApiServiceImpl.queryTableByTableId(" select tsi.hyren_name AS TABLE_NAME," + "ti.table_ch_name ,dta.is_rec from " + DataStoreLayer.TableName + " dsl join " + DtabRelationStore.TableName + " drs on dsl.dsl_id = drs.dsl_id join " + TableStorageInfo.TableName + " tsi on drs.tab_id = tsi.storage_id join " + TableInfo.TableName + " ti on tsi.table_id = ti.table_id join " + DfProInfo.TableName + " dpi on dpi.dsl_id = drs.dsl_id LEFT JOIN " + DfTableApply.TableName + " dta ON dpi.df_pid = dta.df_pid where dpi.df_pid = ? and ti.table_id=?", df_pid, table_id);
            String tableName = resultMap.get("table_name").toString();
            List<List<String>> list = new ArrayList<>();
            List<String> attrList = new ArrayList<>();
            for (DfApiAttr dfApiAttr : listDfAttr) {
                attrList.add(dfApiAttr.getDda_col());
            }
            list.add(attrList);
            List<Map<String, String>> newList = apiDfTableApplyServiceImpl.arrayToObj(list);
            List<String> keyData = apiAttrObjectList.stream().filter(map -> map.get("is_primarykey").equals(true)).map(map -> (String) map.get("dda_col")).collect(Collectors.toList());
            dfTableApplyServiceImpl.createTempTable(keyData, newList, df_pid, table_id, tableName);
            dfApiDef = this.setApplyInfo(dfApiDef, table_id, df_pid);
            apiServiceImpl.checkParams(dfApiDef, listDfAttr);
            apiServiceImpl.saveApi(dfApiDef, listDfAttr);
        } else {
            dfApiDef = this.setApplyInfo(dfApiDef, table_id, df_pid);
            apiServiceImpl.checkParams(dfApiDef, listDfAttr);
            apiServiceImpl.saveApi(dfApiDef, listDfAttr);
        }
    }

    public DfApiDef setApplyInfo(DfApiDef dfApiDef, Long table_id, Long dfPid) {
        DfTableApply newDfTableApply = apiDfTableApplyServiceImpl.getDfTableApplyById(table_id, dfPid);
        dfApiDef.setApply_tab_id(newDfTableApply.getApply_tab_id());
        dfApiDef.setTable_name(newDfTableApply.getDsl_table_name_id());
        return dfApiDef;
    }

    @ApiOperation(value = "")
    @PostMapping("/updateApi")
    public void updateApi(@RequestBody Map<String, Object> requestData) {
        Map<String, Object> dataFillObjectMap = (Map<String, Object>) requestData.get("dfApiDefUpdate");
        Long api_id = Long.valueOf(dataFillObjectMap.get("api_id").toString());
        Map<String, Object> apiDef = apiServiceImpl.getApi("select * FROM " + DfApiDef.TableName + " WHERE api_id = ?", api_id);
        DfTableApply dfTableApply = apiDfTableApplyServiceImpl.getDfTableApplyByApiId(Long.parseLong((String.valueOf(apiDef.get("apply_tab_id")))));
        if (dfTableApply.getApply_tab_id() != null) {
            Long table_id = dfTableApply.getTable_id();
            Long df_pid = dfTableApply.getDf_pid();
            DfApiDef dfApiDef = new DfApiDef();
            dfApiDef.setApi_id(Long.valueOf(dataFillObjectMap.get("api_id").toString()));
            this.setDfApiDef(dataFillObjectMap, dfApiDef);
            this.setApplyInfo(dfApiDef, table_id, df_pid);
            apiServiceImpl.upDateDfApiDef(dfApiDef);
        } else {
            throw new SystemBusinessException("修改接口时不能修改目标表和项目信息");
        }
    }

    public void setDfApiDef(Map<String, Object> dataFillObjectMap, DfApiDef dfApiDef) {
        if (dataFillObjectMap.get("api_cn_name") != null && dataFillObjectMap.get("api_cn_name") != "") {
            dfApiDef.setApi_cn_name(dataFillObjectMap.get("api_cn_name").toString());
        }
        dfApiDef.setApi_name(dataFillObjectMap.get("api_name").toString());
        dfApiDef.setApi_create_date(DateUtil.getSysDate());
        dfApiDef.setApi_create_time(DateUtil.getSysTime());
        dfApiDef.setApi_state(Status.TRUE.getCode());
        dfApiDef.setApi_ip(dataFillObjectMap.get("api_ip").toString());
        dfApiDef.setApi_port(Integer.parseInt(dataFillObjectMap.get("api_port").toString()));
        if (dataFillObjectMap.get("api_remarks") != null) {
            dfApiDef.setApi_remarks(dataFillObjectMap.get("api_remarks").toString());
        } else {
            dfApiDef.setApi_remarks("无");
        }
    }

    public void setDfApiAttr(Map<String, Object> apiAttrObjectMap, DfApiAttr dfApiAttr) {
        dfApiAttr.setDda_col(apiAttrObjectMap.get("dda_col").toString().toLowerCase());
        if (apiAttrObjectMap.get("data_type") != null && apiAttrObjectMap.get("data_type") != "") {
            dfApiAttr.setCol_type(apiAttrObjectMap.get("data_type").toString());
        } else {
            throw new BusinessException("字段类型有误");
        }
        if (apiAttrObjectMap.get("column_ch_name") != null && apiAttrObjectMap.get("column_ch_name") != "") {
            dfApiAttr.setCol_name(apiAttrObjectMap.get("column_ch_name").toString());
        } else {
            dfApiAttr.setCol_name("中文名缺失");
        }
        if (apiAttrObjectMap.get("dda_remarks") != null && apiAttrObjectMap.get("dda_remarks") != "") {
            dfApiAttr.setDda_remarks(apiAttrObjectMap.get("dda_remarks").toString());
        } else {
            dfApiAttr.setDda_remarks("无");
        }
        if (apiAttrObjectMap.get("is_primarykey") != null && apiAttrObjectMap.get("is_primarykey").equals(true)) {
            dfApiAttr.setIs_primarykey(IsFlag.Shi.getCode());
        } else {
            dfApiAttr.setIs_primarykey(IsFlag.Fou.getCode());
        }
    }

    @ApiOperation(value = "")
    @PostMapping("/deleteApi")
    public void deleteApi(String apiId) {
        DfApiDef dfApiDef = MapUtil.toObject(apiServiceImpl.getApi("select * FROM " + DfApiDef.TableName + " WHERE api_id = ? ", Long.parseLong(apiId)), DfApiDef.class);
        dfApiDef.setApi_state("F");
        apiServiceImpl.upDateDfApiDef(dfApiDef);
    }

    @ApiOperation(value = "")
    @PostMapping("/getApiByName")
    public DfApiDef getApiByName(String apiName) {
        return MapUtil.toObject(apiServiceImpl.getApi("select * FROM " + DfApiDef.TableName + " WHERE api_name = ? ", apiName), DfApiDef.class);
    }

    @ApiOperation(value = "")
    @PostMapping("/getApiDetils")
    public Map<String, Object> getApiDetils(String apiId) {
        Map<String, Object> dfApiDef = apiServiceImpl.getApi("select t1.*,t2.dsl_name From (SELECT a.*, p.DSL_ID,p.pro_name FROM " + DfApiDef.TableName + " a left JOIN " + DfTableApply.TableName + " t ON a.APPLY_TAB_ID = t.APPLY_TAB_ID left JOIN " + DfProInfo.TableName + " p ON t.DF_PID = p.DF_PID) t1 left join " + DataStoreLayer.TableName + " t2 on t1.dsl_id = t2.dsl_id" + " where t1.api_id=?", Long.parseLong(apiId));
        List<Map<String, Object>> dfAttr = apiServiceImpl.searchApiAttr("select dda_col,col_name as column_ch_name, col_type as data_type, is_primarykey from " + DfApiAttr.TableName + " where api_id=?", Long.parseLong(apiId)).toList();
        dfApiDef.put("dfAttr", dfAttr);
        return dfApiDef;
    }

    @ApiOperation(value = "")
    @PostMapping("/getDfProInfoList")
    public Result getDfProInfoList() {
        return apiDfProInfoApiServiceImpl.searchDfProInfoApi("select * from " + DfProInfo.TableName + " where submit_state in(?,?)", DFAppState.CaoGao.getCode(), DFAppState.YiJuJue.getCode());
    }

    @ApiOperation(value = "")
    @PostMapping("/getTableLayerByDfPId")
    public LayerBean getTableLayerByDslId(@RequestParam String df_pid) {
        DfProInfo dfProInfo = apiDfProInfoApiServiceImpl.queryDfProInfoById("select * from " + DfProInfo.TableName + " where df_pid =? ", Long.parseLong(df_pid)).orElseThrow(() -> new BusinessException("未查询到DfProInfo对象"));
        if (dfProInfo.getDsl_id() == null) {
            throw new SystemBusinessException("查询补录项目信息异常");
        } else {
            LayerBean layerBean = dataLayerServiceImpl.getTableLayerByDslId(dfProInfo.getDsl_id());
            if (layerBean.getDsl_id() == null) {
                throw new SystemBusinessException("获取存储层数据信息的SQL执行失败");
            } else {
                return layerBean;
            }
        }
    }

    @ApiOperation(value = "")
    @PostMapping("/queryDataBasedOnApplyTabId")
    public List<Map<String, Object>> queryDataBasedOnApplyTabId(@RequestParam String table_id) {
        return apiDfProInfoApiServiceImpl.queryDataBasedOnTableId(Long.parseLong(table_id));
    }

    @ApiOperation(value = "")
    @PostMapping("/getTableApplyList")
    public Result getTableApplyList(@RequestParam String df_pid) {
        DfProInfo dfProInfo = apiDfProInfoApiServiceImpl.queryDfProInfoById("select * from " + DfProInfo.TableName + " where df_pid =? ", Long.parseLong(df_pid)).orElseThrow(() -> new BusinessException("未查询到DfProInfo对象"));
        return apiDfTableApplyServiceImpl.getDfTableApplyList("select ti.table_name ,* from " + DataStoreLayer.TableName + " dsl inner join " + DtabRelationStore.TableName + " drs on dsl.dsl_id = drs.dsl_id inner join " + TableStorageInfo.TableName + " tsi on drs.tab_id = tsi.storage_id inner join " + TableInfo.TableName + " ti on tsi.table_id = ti.table_id where dsl.dsl_id  =?", dfProInfo.getDsl_id());
    }

    @ApiOperation(value = "")
    @PostMapping("/getDfTableApplyList")
    public Result getDfTableApplyList(@RequestParam String df_pid) {
        return apiDfTableApplyServiceImpl.getDfTableApplyList("select t1.df_pid, t1.table_id,t1.apply_tab_id, t2.table_name from " + DfTableApply.TableName + " t1 left join " + TableInfo.TableName + " t2 on t1.table_id =t2.table_id " + "WHERE df_pid=?", Long.parseLong(df_pid));
    }
}
