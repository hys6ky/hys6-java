package hyren.serv6.agent.job.biz.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.daos.base.utils.ActionResult;
import hyren.serv6.base.entity.CollectCase;
import hyren.serv6.base.entity.DataStoreReg;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.AgentActionUtil;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.base.utils.packutil.PackUtil;
import lombok.extern.slf4j.Slf4j;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/11/22 10:04")
public class CommunicationUtil {

    static {
        File file = new File(Constant.COMMUNICATIONERRORFOLDER);
        if (!file.exists()) {
            if (!file.mkdir()) {
                throw new AppSystemException("创建文件夹" + Constant.COMMUNICATIONERRORFOLDER + "失败");
            }
        }
    }

    public static void saveCollectCase(CollectCase collect_case, String loadMessage) {
        try {
            String url = AgentActionUtil.getServerUrl(AgentActionUtil.SAVECOLLECTCASE);
            HttpClient.ResponseValue resVal = new HttpClient().addData("collect_case", PackUtil.packMsg(JsonUtil.toJson(collect_case))).addData("msg", StringUtil.isBlank(loadMessage) ? "excption is null " : loadMessage).post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new AppSystemException(String.format("保存错误信息失败:%s", ar.getMessage()));
            }
        } catch (Exception e) {
            log.error("保存采集情况信息表信息失败：", e);
            Map<String, Object> object = new HashMap<>();
            object.put("collect_case", JsonUtil.toJson(collect_case));
            object.put("msg", StringUtil.isBlank(loadMessage) ? "excption is null " : loadMessage);
            writeCommunicationErrorFile(AgentActionUtil.SAVECOLLECTCASE, object.toString(), e.getMessage(), collect_case.getJob_rs_id());
        }
    }

    public static void batchAddSourceFileAttribute(List<Object[]> addParamsPool, String addSql, String job_rs_id) {
        try {
            String url = AgentActionUtil.getServerUrl(AgentActionUtil.BATCHADDSOURCEFILEATTRIBUTE);
            HttpClient.ResponseValue resVal = new HttpClient().addData("addSql", addSql).addData("addParamsPool", PackUtil.packMsg(JsonUtil.toJson(addParamsPool))).post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new AppSystemException(String.format("agent连接服务端批量添加source_file_attribute信息异常:%s", ar.getMessage()));
            }
        } catch (Exception e) {
            log.error("批量添加SourceFileAttribute表信息失败：", e);
            Map<String, Object> object = new HashMap<>();
            object.put("addSql", addSql);
            object.put("addParamsPool", JsonUtil.toJson(addParamsPool));
            writeCommunicationErrorFile(AgentActionUtil.BATCHADDSOURCEFILEATTRIBUTE, object.toString(), e.getMessage(), job_rs_id);
        }
    }

    public static void batchAddFtpTransfer(List<Object[]> addParamsPool, String addSql, String job_rs_id) {
        try {
            String url = AgentActionUtil.getServerUrl(AgentActionUtil.BATCHADDFTPTRANSFER);
            HttpClient.ResponseValue resVal = new HttpClient().addData("addSql", addSql).addData("addParamsPool", PackUtil.packMsg(JsonUtil.toJson(addParamsPool))).post(url);
            ActionResult actionResult = ActionResult.toActionResult(resVal.getBodyString());
            if (!actionResult.isSuccess()) {
                throw new AppSystemException(String.format("agent连接服务端批量添加ftp_transfered信息异常:%s", actionResult.getMessage()));
            }
        } catch (Exception e) {
            log.error("批量添加ftp_transfered(ftp已传输)表信息失败", e);
            Map<String, Object> object = new HashMap<>();
            object.put("addSql", addSql);
            object.put("addParamsPool", JsonUtil.toJson(addParamsPool));
            writeCommunicationErrorFile(AgentActionUtil.BATCHADDFTPTRANSFER, object.toString(), e.getMessage(), job_rs_id);
        }
    }

    public static void batchUpdateSourceFileAttribute(List<Object[]> updateParamsPool, String updateSql, String job_rs_id) {
        try {
            String url = AgentActionUtil.getServerUrl(AgentActionUtil.BATCHUPDATESOURCEFILEATTRIBUTE);
            HttpClient.ResponseValue resVal = new HttpClient().addData("updateSql", updateSql).addData("updateParamsPool", PackUtil.packMsg(JsonUtil.toJson(updateParamsPool))).post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new AppSystemException(String.format("agent连接服务端批量更新source_file_attribute信息异常:%s", ar.getMessage()));
            }
        } catch (Exception e) {
            log.error("批量更新SourceFileAttribute表信息失败", e);
            Map<String, Object> object = new HashMap<>();
            object.put("updateSql", updateSql);
            object.put("updateParamsPool", JsonUtil.toJson(updateParamsPool));
            writeCommunicationErrorFile(AgentActionUtil.BATCHUPDATESOURCEFILEATTRIBUTE, object.toString(), e.getMessage(), job_rs_id);
        }
    }

    private static void writeCommunicationErrorFile(String methodName, String param, String errorMsg, String job_rs_id) {
        Map<String, Object> object = new HashMap<>();
        object.put("methodName", methodName);
        object.put("param", param);
        object.put("errorMsg", errorMsg);
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(Constant.COMMUNICATIONERRORFOLDER + job_rs_id + ".error", true)));
            out.write(object.toString());
            out.newLine();
        } catch (Exception e) {
            log.error("写错误文件失败：", e);
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                log.error("关闭流失败：", e);
            }
        }
    }

    public static void addDataStoreReg(DataStoreReg data_store_reg, String database_id) {
        try {
            String url = AgentActionUtil.getServerUrl(AgentActionUtil.ADDDATASTOREREG);
            HttpClient.ResponseValue resVal = new HttpClient().addData("data_store_reg", PackUtil.packMsg(JsonUtil.toJson(data_store_reg))).post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new AppSystemException(String.format("agent连接服务端批量更新data_store_reg信息异常:%s", ar.getMessage()));
            }
        } catch (Exception e) {
            Map<String, Object> object = new HashMap<>();
            object.put("data_store_reg", JsonUtil.toJson(data_store_reg));
            writeCommunicationErrorFile(AgentActionUtil.ADDDATASTOREREG, object.toString(), e.getMessage(), database_id);
        }
    }
}
