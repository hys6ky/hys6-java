package hyren.serv6.b.batchcollection.semiStructuredAgent.collectconf;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.tools.CommonUtils;
import hyren.serv6.b.agent.tools.SendMsgUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.ObjectCollectType;
import hyren.serv6.base.entity.AgentInfo;
import hyren.serv6.base.entity.ObjectCollect;
import hyren.serv6.base.entity.ObjectCollectTask;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.AgentActionUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@DocClass(desc = "", author = "dhw", createdate = "2020/6/9 10:36")
public class CollectConfService {

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getInitObjectCollectConf(Long agent_id) {
        String url = AgentActionUtil.getUrl(agent_id, UserUtil.getUserId(), AgentActionUtil.GETSERVERINFO);
        try {
            HttpClient.ResponseValue resVal = new HttpClient().get(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new BusinessException("远程连接" + url + "的Agent失败");
            }
            Map map = (Map<String, Object>) ar.getData();
            map.put("localdate", DateUtil.getSysDate());
            map.put("localtime", DateUtil.getSysTime());
            return map;
        } catch (Exception e) {
            throw new BusinessException("与Agent端服务交互异常!!!" + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getAddObjectCollectConf(Long agent_id) {
        return Dbo.queryOneObject("SELECT t1.*" + " FROM " + ObjectCollect.TableName + " t1 " + " LEFT JOIN " + AgentInfo.TableName + " t2 ON t1.Agent_id = t2.Agent_id " + " WHERE t1.Agent_id = ? AND t1.is_sendok = ?", agent_id, IsFlag.Fou.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getObjectCollectConfById(long odc_id) {
        CommonUtils.isObjectCollectExist(odc_id);
        return Dbo.queryOneObject("SELECT * FROM " + ObjectCollect.TableName + " WHERE odc_id = ?", odc_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "file_path", desc = "", range = "")
    @Param(name = "is_dictionary", desc = "", range = "")
    @Param(name = "data_date", desc = "", range = "", nullable = true)
    @Param(name = "file_suffix", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<ObjectCollectTask> viewTable(long agent_id, String file_path, String is_dictionary, String data_date, String file_suffix) {
        if (IsFlag.Fou == IsFlag.ofEnumByCode(is_dictionary) && StringUtil.isBlank(data_date)) {
            throw new BusinessException("当是否存在数据字典选择否，数据日期不能为空");
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(is_dictionary)) {
            return SendMsgUtil.getDictionaryTableInfo(agent_id, file_path, UserUtil.getUserId());
        } else {
            return SendMsgUtil.getFirstLineData(agent_id, file_path, data_date, file_suffix, UserUtil.getUserId());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "object_collect", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public long saveObjectCollect(ObjectCollect object_collect) {
        object_collect.setObject_collect_type(ObjectCollectType.HangCaiJi.getCode());
        if (IsFlag.Fou == IsFlag.ofEnumByCode(object_collect.getIs_dictionary()) && StringUtil.isBlank(object_collect.getData_date())) {
            throw new BusinessException("当是否存在数据字典选择否的时候，数据日期不能为空");
        }
        if (IsFlag.Shi == IsFlag.ofEnumByCode(object_collect.getIs_dictionary())) {
            object_collect.setData_date("");
        }
        if (null == object_collect.getServer_date()) {
            object_collect.setServer_date(StringUtil.EMPTY);
        }
        if (object_collect.getOdc_id() == null) {
            isObjNumberExist(object_collect.getObj_number());
            object_collect.setOdc_id(PrimayKeyGener.getNextId());
            object_collect.setIs_sendok(IsFlag.Fou.getCode());
            object_collect.add(Dbo.db());
        } else {
            try {
                object_collect.update(Dbo.db());
            } catch (Exception e) {
                if (!(e instanceof ProEntity.EntityDealZeroException)) {
                    throw new BusinessException(e.getMessage());
                }
            }
        }
        return object_collect.getOdc_id();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "obj_number", desc = "", range = "")
    private void isObjNumberExist(String obj_number) {
        long count = Dbo.queryNumber("SELECT count(1) count FROM " + ObjectCollect.TableName + " WHERE obj_number = ?", obj_number).orElseThrow(() -> new BusinessException("sql查询错误"));
        if (count > 0) {
            throw new BusinessException("半结构化采集任务编号重复");
        }
    }
}
