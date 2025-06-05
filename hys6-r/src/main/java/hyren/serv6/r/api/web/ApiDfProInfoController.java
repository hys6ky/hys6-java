package hyren.serv6.r.api.web;

import fd.ng.core.utils.MapUtil;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.codes.DFAppState;
import hyren.serv6.base.codes.DFType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.DfProInfo;
import hyren.serv6.base.entity.DfTableApply;
import hyren.serv6.r.api.service.ApiDfProInfoApiServiceImpl;
import hyren.serv6.r.api.service.ApiDfTableApplyServiceImpl;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("dataSupplementStatistics/dfProInfoApi")
@Api(tags = "")
@Validated
public class ApiDfProInfoController {

    @Autowired
    ApiDfProInfoApiServiceImpl apiDfProInfoApiServiceImpl;

    @Autowired
    ApiDfTableApplyServiceImpl apiDfTableApplyServiceImpl;

    @ApiOperation(value = "")
    @PostMapping("/getDfProInfoList")
    public Result getDfProInfoList() {
        return apiDfProInfoApiServiceImpl.searchDfProInfoApi("select * from " + DfProInfo.TableName + " where submit_state in(?,?)", DFAppState.CaoGao.getCode(), DFAppState.YiJuJue.getCode());
    }

    @ApiOperation(value = "")
    @PostMapping("/queryDataBasedOnApplyTabId")
    public List<Map<String, Object>> queryDataBasedOnApplyTabId(@RequestParam String table_id) {
        return apiDfProInfoApiServiceImpl.queryDataBasedOnTableId(Long.parseLong(table_id));
    }

    @ApiOperation(value = "")
    @PostMapping("/queryDfTableApplyByApplyTabId")
    public DfTableApply queryDfTableApplyById(String applyTabId) {
        return MapUtil.toObject(apiDfProInfoApiServiceImpl.queryDfTableApplyById("select * from" + DfTableApply.TableName + "where applyTabId = ? ", applyTabId), DfTableApply.class);
    }

    @ApiOperation(value = "")
    @PostMapping("/getDfProInfoListByDfType")
    public List<Map<String, Object>> getDfProInfoListByDfType(String startDate, String endDate, String dfType) {
        SqlOperator.Assembler amSql = SqlOperator.Assembler.newInstance();
        amSql.clean();
        amSql.addSql("SELECT df_type as name, COUNT (*) AS value, " + "ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),2) AS percentage FROM ").addSql(DfProInfo.TableName);
        amSql.addSql(" WHERE df_type IS NOT NULL AND df_type <> '' ");
        if (startDate != null && endDate != null) {
            amSql.addSql(" and submit_date >= ? and submit_date <= ? ");
        }
        if (dfType != null && !dfType.equals("")) {
            amSql.addSql("and df_type=? ");
        }
        amSql.addSql("GROUP BY  df_type ORDER BY  df_type, percentage desc");
        Result result = apiDfProInfoApiServiceImpl.getDfProInfoListByDfType(amSql.sql(), startDate, endDate, dfType);
        return this.setLabel(result);
    }

    @ApiOperation(value = "")
    @PostMapping("/getDfProInfoListByDfTypeAndMonth")
    public List<Map<String, Object>> getDfProInfoListByDfTypeAndMonth(String startDate, String endDate, String dfType) {
        SqlOperator.Assembler amSql = SqlOperator.Assembler.newInstance();
        amSql.clean();
        amSql.addSql("SELECT df_type as name, COUNT (*) AS value, TO_CHAR(DATE_TRUNC('month', submit_date ::timestamp), " + "'MM') AS month FROM ").addSql(DfProInfo.TableName);
        amSql.addSql(" WHERE df_type IS NOT NULL AND df_type <> '' ");
        if (startDate != null && endDate != null) {
            amSql.addSql(" and submit_date >= ? and submit_date <= ? ");
        }
        if (dfType != null && !dfType.equals("")) {
            amSql.addSql("and df_type=? ");
        }
        amSql.addSql("GROUP BY month, df_type ORDER BY month, df_type desc");
        Result result = apiDfProInfoApiServiceImpl.getDfProInfoListByDfType(amSql.sql(), startDate, endDate, dfType);
        return this.setLabel(result);
    }

    private List<Map<String, Object>> setLabel(Result result) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        for (Map<String, Object> map : result.toList()) {
            if (map.get("name").equals(DFType.JiangGuan.getCode())) {
                map.put("label", DFType.JiangGuan.getValue());
            } else if (map.get("name").equals(DFType.ChangGui.getCode())) {
                map.put("label", DFType.ChangGui.getValue());
            } else if (map.get("name").equals(DFType.LinShi.getCode())) {
                map.put("label", DFType.LinShi.getValue());
            }
            resultList.add(map);
        }
        return resultList;
    }

    @ApiOperation(value = "")
    @PostMapping("/getDfProInfoARAndSuNumber")
    public Result getDfProInfoARAndSuNumber(String startDate, String endDate) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT COUNT (*) AS value, TO_CHAR(DATE_TRUNC('month', submit_date ::timestamp), 'MM') AS month FROM ");
        asmSql.addSql(DfProInfo.TableName);
        asmSql.addSql(" WHERE SUBMIT_STATE ='" + DFAppState.YiShenPi.getCode() + "'");
        if (startDate != null && endDate != null) {
            asmSql.addSql(" and submit_date >= ? and submit_date <= ? ");
        }
        asmSql.addSql("GROUP BY month ORDER BY  month");
        Result result = apiDfProInfoApiServiceImpl.getDfProInfoListByDfType(asmSql.sql(), startDate, endDate);
        asmSql.clean();
        asmSql.addSql("SELECT COUNT (*) AS value, TO_CHAR(DATE_TRUNC('month', submit_date ::timestamp), 'MM') AS month FROM ");
        asmSql.addSql(DfProInfo.TableName);
        if (startDate != null && endDate != null) {
            asmSql.addSql(" where submit_date >= ? and submit_date <= ? ");
        }
        asmSql.addSql("GROUP BY month ORDER BY  month");
        Result result1 = apiDfProInfoApiServiceImpl.getDfProInfoListByDfType(asmSql.sql(), startDate, endDate);
        for (int i = 0; i < result1.getRowCount(); i++) {
            if (!result.isEmpty()) {
                for (int j = 0; j < result.getRowCount(); j++) {
                    result1.setValue(i, "addValue", result.getString(j, "value"));
                }
            } else {
                result1.setValue(i, "addValue", "0");
            }
        }
        return result1;
    }

    @ApiOperation(value = "")
    @PostMapping("/getDfApplyNumber")
    public Result getDfApplyNumber(String startDate, String endDate) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql(" SELECT EXTRACT( MONTH FROM TO_TIMESTAMP ( SYNC_DATE, 'YYYYMMDD' ) ) AS MONTH, COUNT( * ) AS counts," + "ROUND(CAST(AVG(DATE_PART('day', TO_TIMESTAMP(SYNC_DATE, 'YYYYMMDD') - TO_TIMESTAMP(CREATE_DATE, 'YYYYMMDD'))) AS DECIMAL(10,2)), 2) AS avg_days_to_sync FROM ");
        asmSql.addSql(DfTableApply.TableName);
        asmSql.addSql(" WHERE IS_SYNC = '" + IsFlag.Shi.getCode() + "'");
        if (null != startDate && null != endDate) {
            asmSql.addSql(" and sync_date >= ? and sync_date <= ? ");
        }
        asmSql.addSql(" GROUP BY EXTRACT( MONTH FROM TO_TIMESTAMP ( SYNC_DATE, 'YYYYMMDD' ) )");
        return apiDfProInfoApiServiceImpl.getDfProInfoListByDfType(asmSql.sql(), startDate, endDate);
    }

    @ApiOperation(value = "")
    @PostMapping("/getDfExamineNumber")
    public Result getDfExamineNumber(String startDate, String endDate) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT EXTRACT( MONTH FROM TO_TIMESTAMP ( AUDIT_DATE, 'YYYYMMDD' ) ) AS MONTH, COUNT( * ) AS counts," + "AVG( DATE_PART ( 'day', TO_TIMESTAMP ( AUDIT_DATE, 'YYYYMMDD' ) - TO_TIMESTAMP ( SUBMIT_DATE, 'YYYYMMDD' ) ) ) AS avg_days_to_sync FROM ");
        asmSql.addSql(DfProInfo.TableName);
        asmSql.addSql(" WHERE SUBMIT_STATE ='" + DFAppState.YiShenPi.getCode() + "'");
        if (null != startDate && null != endDate) {
            asmSql.addSql(" and audit_date >= ? and audit_date <= ? ");
        }
        asmSql.addSql(" GROUP BY EXTRACT( MONTH FROM TO_TIMESTAMP ( AUDIT_DATE, 'YYYYMMDD' ) )");
        return apiDfProInfoApiServiceImpl.getDfProInfoListByDfType(asmSql.sql(), startDate, endDate);
    }
}
