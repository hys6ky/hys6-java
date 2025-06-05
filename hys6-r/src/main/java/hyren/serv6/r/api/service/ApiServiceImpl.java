package hyren.serv6.r.api.service;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.Status;
import hyren.serv6.base.entity.DfApiAttr;
import hyren.serv6.base.entity.DfApiDef;
import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ApiServiceImpl {

    @Autowired
    ApiDfProInfoApiServiceImpl apiDfProInfoApiServiceImpl;

    public Result searchApi(String sql, Object... params) {
        return Dbo.queryResult(sql, params);
    }

    public Result searchApiAttr(String sql, Object... params) {
        return Dbo.queryResult(sql, params);
    }

    public Map<String, Object> getApi(String sql, Object... params) {
        return Dbo.queryOneObject(sql, params);
    }

    public void saveDfApiDef(DfApiDef dfApiDef) {
        this.checkBlank(dfApiDef);
        dfApiDef.add(Dbo.db());
    }

    public void saveDfApiAttr(DfApiAttr dfApiAttr) {
        if (StringUtil.isBlank(dfApiAttr.getDaa_id().toString())) {
            throw new BusinessException("id不能为空，daa_id=" + dfApiAttr.getDaa_id());
        }
        dfApiAttr.add(Dbo.db());
    }

    public void saveApi(DfApiDef dfApiDef, List<DfApiAttr> listDfAttr) {
        this.saveDfApiDef(dfApiDef);
        if (this.checkPrimaryKey(listDfAttr, dfApiDef)) {
            for (DfApiAttr dfApiAttr : listDfAttr) {
                this.saveDfApiAttr(dfApiAttr);
            }
        }
    }

    public void upDateDfApiDef(DfApiDef dfApiDef) {
        this.checkBlank(dfApiDef);
        dfApiDef.update(Dbo.db());
    }

    public void checkBlank(DfApiDef dfApiDef) {
        if (StringUtil.isBlank(dfApiDef.getApi_id().toString())) {
            throw new BusinessException("id不能为空，api_id=" + dfApiDef.getApi_ip());
        }
        if (StringUtil.isBlank(dfApiDef.getApply_tab_id().toString())) {
            throw new BusinessException("申请表id不能为空，applyTable_id=" + dfApiDef.getApply_tab_id());
        }
        if (StringUtil.isBlank(dfApiDef.getApi_name())) {
            throw new BusinessException("api名称不能为空，api_name=" + dfApiDef.getApi_name());
        }
        if (StringUtil.isBlank(dfApiDef.getTable_name())) {
            throw new BusinessException("接口对应的表名不能为空，table_name=" + dfApiDef.getTable_name());
        }
        if (StringUtil.isBlank(dfApiDef.getApi_ip())) {
            throw new BusinessException("ip不能为空，api_ip=" + dfApiDef.getApi_ip());
        }
        if (StringUtil.isBlank(dfApiDef.getApi_port().toString())) {
            throw new BusinessException("id不能为空，api_port=" + dfApiDef.getApi_port());
        }
    }

    public void checkParams(DfApiDef dfApiDef, List<DfApiAttr> listDfAttr) {
        Map<String, Object> dfApiDe = Dbo.queryOneObject("select api_name from " + DfApiDef.TableName + " where apply_tab_id=? and api_state=?", Long.parseLong(dfApiDef.getApply_tab_id().toString()), Status.TRUE.getCode());
        if (!dfApiDe.isEmpty()) {
            if (dfApiDe.get("api_name").equals(dfApiDef.getApi_name())) {
                throw new BusinessException("接口名称不能重复");
            }
        }
        List<Map<String, Object>> dfApiAttrList = this.searchApiAttr("select a.dda_col from " + DfApiAttr.TableName + " a left join " + DfApiDef.TableName + " b on a.api_id=b.api_id where apply_tab_id=? and api_state=?", Long.parseLong(dfApiDef.getApply_tab_id().toString()), Status.TRUE.getCode()).toList();
        if (!dfApiAttrList.isEmpty()) {
            for (DfApiAttr dfApiAttrs : listDfAttr) {
                for (Map<String, Object> attr : dfApiAttrList) {
                    if (!dfApiAttrs.getDda_col().equals(attr.get("dda_col"))) {
                        return;
                    } else {
                        throw new BusinessException("该表已存在一个类似接口");
                    }
                }
            }
        }
    }

    public boolean checkPrimaryKey(List<DfApiAttr> listDfAttr, DfApiDef dfApiDef) {
        List<Map<String, Object>> priMaryKey = apiDfProInfoApiServiceImpl.queryPrimaryKeyOnTableName(dfApiDef.getApply_tab_id());
        Iterator<Map<String, Object>> iterator = priMaryKey.iterator();
        while (iterator.hasNext()) {
            Map<String, Object> element = iterator.next();
            String value = String.valueOf(element.get("dda_col"));
            switch(value) {
                case "hyren_s_date":
                case "hyren_oper_time":
                case "hyren_md5_val":
                case "hyren_oper_person":
                case "hyren_oper_date":
                case "hyren_e_date":
                    iterator.remove();
                    break;
                default:
                    break;
            }
        }
        Integer countKey = 0;
        List<DfApiAttr> nowKeys = new ArrayList<>();
        for (DfApiAttr dfApiAttr : listDfAttr) {
            if (dfApiAttr.getIs_primarykey().equals(IsFlag.Shi.getCode())) {
                nowKeys.add(dfApiAttr);
            }
        }
        if (priMaryKey.size() > 0) {
            if (nowKeys.size() == 0) {
                throw new BusinessException("临时表不能没有主键信息，请定义后重新提交");
            }
            for (Map<String, Object> map : priMaryKey) {
                for (DfApiAttr dfApiAttr : nowKeys) {
                    String str1 = dfApiAttr.getDda_col().toLowerCase();
                    String str2 = map.get("dda_col").toString().toLowerCase();
                    if (str2.equals(str1)) {
                        countKey++;
                    }
                }
            }
            if (nowKeys.size() == 0) {
                throw new BusinessException("临时表不能没有主键信息，请定义后重新提交");
            } else if (priMaryKey.size() != countKey) {
                throw new BusinessException("临时表主键与选择主键不一致，临时表主键字段为:" + priMaryKey);
            }
            return priMaryKey.size() == countKey;
        }
        return false;
    }
}
