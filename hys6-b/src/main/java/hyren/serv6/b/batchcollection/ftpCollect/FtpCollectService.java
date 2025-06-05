package hyren.serv6.b.batchcollection.ftpCollect;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.FtpCollect;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.AgentActionUtil;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import static hyren.serv6.base.user.UserUtil.getUserId;

@Slf4j
@Service
@Api("Ftp采集前端接口类，处理ftp采集的增改查")
@DocClass(desc = "", author = "zxz", createdate = "2019/9/16 17:55")
public class FtpCollectService {

    @Method(desc = "", logicStep = "")
    @Param(name = "ftp_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public FtpCollect searchFtp_collect(long ftp_id) {
        FtpCollect ftp_collect = Dbo.queryOneObject(FtpCollect.class, "SELECT * FROM " + FtpCollect.TableName + " WHERE ftp_id = ?", ftp_id).orElseThrow(() -> new BusinessException("根据ftp_id:" + ftp_id + "查询不到ftp_collect表信息"));
        ftp_collect.setFtp_password(StringUtil.unicode2String(ftp_collect.getFtp_password()));
        return ftp_collect;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ftp_collect", desc = "", range = "", isBean = true)
    public void addFtp_collect(FtpCollect ftp_collect) {
        validatorData(ftp_collect);
        long count = Dbo.queryNumber("SELECT count(1) count FROM " + FtpCollect.TableName + " WHERE ftp_name = ?", ftp_collect.getFtp_name()).orElseThrow(() -> new BusinessException("查询得到的数据必须有且只有一条"));
        if (count > 0) {
            throw new BusinessException("ftp采集任务名称重复");
        } else {
            ftp_collect.setFtp_id(PrimayKeyGener.getNextId());
            ftp_collect.setIs_sendok(IsFlag.Shi.getCode());
            ftp_collect.setFtp_password(StringUtil.string2Unicode(ftp_collect.getFtp_password()));
            ftp_collect.add(Dbo.db());
        }
        sendFtp_collect(ftp_collect);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ftp_collect", desc = "", range = "", isBean = true)
    public void updateFtp_collect(FtpCollect ftp_collect) {
        if (ftp_collect.getFtp_id() == null) {
            throw new BusinessException("更新ftp_collect时ftp_id不能为空");
        }
        validatorData(ftp_collect);
        ftp_collect.setFtp_password(StringUtil.string2Unicode(ftp_collect.getFtp_password()));
        long count = Dbo.queryNumber("SELECT count(1) count FROM " + FtpCollect.TableName + " WHERE ftp_name = ? AND ftp_id != ?", ftp_collect.getFtp_name(), ftp_collect.getFtp_id()).orElseThrow(() -> new BusinessException("查询得到的数据必须有且只有一条"));
        if (count > 0) {
            throw new BusinessException("更新后的ftp采集任务名称重复");
        } else {
            ftp_collect.setIs_sendok(IsFlag.Shi.getCode());
            ftp_collect.update(Dbo.db());
        }
        sendFtp_collect(ftp_collect);
    }

    @Method(desc = "", logicStep = "")
    private void sendFtp_collect(FtpCollect ftp_collect) {
        if (ftp_collect.getAgent_id() == null) {
            throw new BusinessException("agent_id不能为空");
        }
        String url = AgentActionUtil.getUrl(ftp_collect.getAgent_id(), getUserId(), AgentActionUtil.SENDFTPCOLLECTTASKINFO);
        String ftp_collect_info = JsonUtil.toJson(ftp_collect);
        log.info("配置的ftp采集信息" + ftp_collect_info);
        try {
            HttpClient.ResponseValue resVal = new HttpClient().addData("taskInfo", ftp_collect_info).post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new BusinessException("连接" + url + "失败");
            }
        } catch (Exception e) {
            throw new BusinessException("与Agent端交互异常!!!" + e.getMessage());
        }
    }

    private void validatorData(FtpCollect ftp_collect) {
        Validator.isPort(Integer.parseInt(ftp_collect.getFtp_port()));
        Validator.isIpAddr(ftp_collect.getFtp_ip());
        Validator.notEmpty(ftp_collect.getFtp_name(), "ftp采集任务名称不能为空");
        Validator.notEmpty(ftp_collect.getFtp_number(), "ftp任务编号不能为空");
    }
}
