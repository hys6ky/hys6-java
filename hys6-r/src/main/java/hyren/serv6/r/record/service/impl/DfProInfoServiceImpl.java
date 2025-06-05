package hyren.serv6.r.record.service.impl;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DFAppState;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.fdCode.WebCodesItem;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DfProInfo;
import hyren.serv6.base.entity.DfTableApply;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.User;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.r.record.Res.DfProInfoRes;
import hyren.serv6.r.record.service.DfProInfoService;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("dfProInfoService")
public class DfProInfoServiceImpl implements DfProInfoService {

    public Map<String, Object> queryDfProInfo(Integer currPage, Integer pageSize) {
        Long userId = UserUtil.getUserId();
        String proInfoSql = "SELECT " + "dpi.df_pid," + "dpi.pro_name," + "dpi.df_type," + "dpi.create_user_id," + "dpi.user_id," + "dpi.submit_user," + "dpi.submit_date," + "dpi.submit_time," + "dpi.submit_state," + "dpi.dsl_id," + "COALESCE(dta.count,0) as number " + "FROM " + "df_pro_info dpi " + "LEFT JOIN " + "(select df_pid,count(*) from df_table_apply where is_rec ='" + IsFlag.Shi.getCode() + "' group by df_pid)" + "dta on dpi.df_pid = dta.df_pid " + "WHERE " + "submit_state <> '" + DFAppState.YiShenPi.getCode() + "' " + "AND create_user_id = " + userId + " " + "ORDER BY " + "submit_date DESC," + "submit_time DESC";
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DfProInfoRes> dfProInfoResList = Dbo.queryPagedList(DfProInfoRes.class, page, proInfoSql);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("dfProInfoResList", dfProInfoResList);
        map.put("totalSize", page.getTotalSize());
        return map;
    }

    public Map<String, Object> queryListByNameAOrType(Integer currPage, Integer pageSize, String proName, String dfType) {
        Long userId = UserUtil.getUserId();
        StringBuffer sbSql = new StringBuffer();
        sbSql.append("SELECT " + "dpi.df_pid," + "dpi.pro_name," + "dpi.df_type," + "dpi.create_user_id," + "dpi.user_id," + "dpi.submit_user," + "dpi.submit_date," + "dpi.submit_time," + "dpi.submit_state," + "dpi.dsl_id," + "COALESCE(dta.count,0) as number " + "FROM " + "df_pro_info dpi " + "LEFT JOIN " + "(select df_pid,count(*) from df_table_apply where is_rec ='" + IsFlag.Shi.getCode() + "' group by df_pid)" + "dta on dpi.df_pid = dta.df_pid " + "WHERE " + "submit_state <> '" + DFAppState.YiShenPi.getCode() + "' " + "AND create_user_id = " + userId + " ");
        if (StringUtil.isNotBlank(proName)) {
            sbSql.append("and pro_name like " + "'%" + proName + "%'" + " ");
        }
        if (StringUtil.isNotBlank(dfType)) {
            sbSql.append("and df_type like " + "'%" + dfType + "%'" + " ");
        }
        sbSql.append("order by submit_date desc, submit_time desc");
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<DfProInfoRes> dataList = Dbo.queryPagedList(DfProInfoRes.class, page, sbSql.toString());
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("dfProInfoResList", dataList);
        map.put("totalSize", page.getTotalSize());
        return map;
    }

    public List<Map<String, Object>> queryAllDataLayer() {
        List<Map<String, Object>> dataLayer = Dbo.queryList("SELECT * FROM " + DataStoreLayer.TableName);
        return dataLayer;
    }

    public void saveInfo(DfProInfo dfProInfo) {
        if (StringUtil.isBlank(dfProInfo.getPro_name())) {
            throw new BusinessException("数据补录项目名称不能为空");
        }
        if (!checkDfName(dfProInfo.getPro_name())) {
            throw new BusinessException("数据补录项目名称重复，请重新填写");
        }
        if (StringUtil.isBlank(dfProInfo.getDf_type())) {
            throw new BusinessException("补录类型不能为空");
        }
        if (dfProInfo.getDsl_id() == null) {
            throw new BusinessException("数据源不能为空");
        }
        User user = UserUtil.getUser();
        dfProInfo.setDf_pid(PrimayKeyGener.getNextId());
        dfProInfo.setCreate_user_id(user.getUserId());
        dfProInfo.setUser_id(user.getUserId());
        dfProInfo.setSubmit_user(user.getUsername());
        dfProInfo.setSubmit_date(DateUtil.getSysDate());
        dfProInfo.setSubmit_time(DateUtil.getSysTime());
        dfProInfo.setSubmit_state(DFAppState.CaoGao.getCode());
        dfProInfo.add(Dbo.db());
    }

    private Boolean checkDfName(String name) {
        return Dbo.queryNumber("SELECT COUNT(1) FROM " + DfProInfo.TableName + " WHERE pro_name = ?", name).orElseThrow(() -> new BusinessException("SQL编写错误")) == 0;
    }

    private boolean checkDslId(Long dslId) {
        return Dbo.queryNumber("SELECT COUNT(1) FROM " + DataStoreLayer.TableName + " WHERE dsl_id = ?", dslId).orElseThrow(() -> new BusinessException("检查存储层信息否存在的SQL编写错误")) == 1;
    }

    public void updateInfo(DfProInfo dfProInfo) {
        if (!checkExitByDfproInfo(dfProInfo.getDf_pid())) {
            throw new BusinessException("根据项目id未查询到该项目信息");
        }
        if (queryDfProInfoById(dfProInfo.getDf_pid()).getSubmit_state().equals(DFAppState.YiShenPi.getCode())) {
            throw new BusinessException("此项目审批状态为通过不能修改该项目信息");
        }
        dfProInfo.setSubmit_date(DateUtil.getSysDate());
        dfProInfo.setSubmit_time(DateUtil.getSysTime());
        dfProInfo.update(Dbo.db());
    }

    private boolean checkExitByDfproInfo(Long df_Pid) {
        return Dbo.queryNumber("select count(1) from " + DfProInfo.TableName + " where df_pid = ?", df_Pid).orElseThrow(() -> new BusinessException("检查项目补录信息表信息是否存在的SQL编写错误！")) == 1;
    }

    public void deleteInfoByPid(Long dfPid) {
        if (!checkExitByDfproInfo(dfPid)) {
            throw new BusinessException("根据临时补录项目ID无法查询到该项目 + dfPid = " + dfPid);
        }
        if (queryDfProInfoById(dfPid).getSubmit_state() == DFAppState.YiShenPi.getCode() || queryDfProInfoById(dfPid).getSubmit_state() == DFAppState.WeiShenPi.getCode()) {
            throw new BusinessException("根据临时补录项目审批状态为已通过，或者已提交审批，不能进行删除 + dfPid = " + dfPid);
        }
        if (checkDfTableApplyByID(dfPid)) {
            throw new BusinessException("根据临时补录项目ID查询到数据补录信息,无法进行删除 + dfPid = " + dfPid);
        }
        DboExecute.deletesOrThrow("删除数据补录项目失败 dfPid=" + dfPid, "delete from " + DfProInfo.TableName + " where df_pid=?", dfPid);
    }

    private DfProInfo queryDfProInfoById(Long dfPid) {
        DfProInfo result = Dbo.queryOneObject(DfProInfo.class, "select * from " + DfProInfo.TableName + " where df_pid=?", dfPid).orElseThrow(() -> new BusinessException("检查数据补录信息表是否存在的SQL编写错误！"));
        return result;
    }

    private boolean checkDfTableApplyByID(Long dfPid) {
        return Dbo.queryNumber("select count(1) from " + DfTableApply.TableName + " where df_pid = ?", dfPid).orElseThrow(() -> new BusinessException("检查数据补录信息表是否存在的SQL编写错误！")) > 0;
    }

    public void updateSubmitStateById(Long dfPid, String submitState) {
        if (!checkExitByDfproInfo(dfPid)) {
            throw new BusinessException("根据临时补录项目ID无法查询到该项目 + dfPid = " + dfPid);
        }
        String tableApplySql = " select df_pid,count(1) as number from df_table_apply where df_pid = " + "'" + dfPid + "'" + " group by df_pid";
        Map<String, Object> countMap = Dbo.queryOneObject(tableApplySql);
        if (countMap.isEmpty()) {
            throw new BusinessException("该项目未进行补录,请通过查看按钮补录后进行提交操作");
        }
        DfProInfo dfProInfo = new DfProInfo();
        if (submitState.equals(DFAppState.CaoGao.getCode()) || submitState.equals(DFAppState.YiJuJue.getCode())) {
            User user = UserUtil.getUser();
            dfProInfo.setDf_pid(dfPid);
            dfProInfo.setUser_id(user.getUserId());
            dfProInfo.setSubmit_user(user.getUsername());
            dfProInfo.setSubmit_date(DateUtil.getSysDate());
            dfProInfo.setSubmit_time(DateUtil.getSysTime());
            dfProInfo.setSubmit_state(DFAppState.WeiShenPi.getCode());
        } else if (submitState.equals(DFAppState.WeiShenPi.getCode())) {
            User user = UserUtil.getUser();
            dfProInfo.setDf_pid(dfPid);
            dfProInfo.setUser_id(user.getUserId());
            dfProInfo.setSubmit_user(user.getUsername());
            dfProInfo.setSubmit_date(DateUtil.getSysDate());
            dfProInfo.setSubmit_time(DateUtil.getSysTime());
            dfProInfo.setSubmit_state(DFAppState.CaoGao.getCode());
        }
        dfProInfo.update(Dbo.db());
    }

    public Result getCategoryItems(String category) {
        return WebCodesItem.getCategoryItems(category);
    }
}
