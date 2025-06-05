package hyren.serv6.agent.trans.biz.semistructured;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.ObjectCollectParamBean;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.agent.job.biz.core.ObjectCollectJobImpl;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.packutil.PackUtil;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.xlstoxml.Xls2xml;
import hyren.serv6.commons.utils.xlstoxml.util.ConnUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
@Service
@DocClass(desc = "", author = "zxz", createdate = "2019/10/23 16:29")
public class ObjectCollectJobService {

    @Method(desc = "", logicStep = "")
    @Param(name = "taskInfo", desc = "", range = "")
    @Return(desc = "", range = "")
    public String execute(String taskInfo) {
        String message = "执行成功";
        try {
            ObjectCollectParamBean objectCollectParamBean = JsonUtil.toObject(PackUtil.unpackMsg(taskInfo).get("msg"), new TypeReference<ObjectCollectParamBean>() {
            });
            FileUtil.createFile(JobConstant.MESSAGEFILE + objectCollectParamBean.getOdc_id(), PackUtil.unpackMsg(taskInfo).get("msg"));
        } catch (Exception e) {
            log.error("对象采集生成配置文件失败:", e);
            message = "对象采集生成配置文件失败:" + e.getMessage();
        }
        return message;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etlDate", desc = "", range = "")
    @Param(name = "taskInfo", desc = "", range = "")
    @Return(desc = "", range = "")
    public String executeImmediately(String etlDate, String taskInfo) {
        String message = "执行成功";
        ExecutorService executor = null;
        try {
            ObjectCollectParamBean objectCollectParamBean = JsonUtil.toObject(PackUtil.unpackMsg(taskInfo).get("msg"), new TypeReference<ObjectCollectParamBean>() {
            });
            FileUtil.createFile(JobConstant.MESSAGEFILE + objectCollectParamBean.getOdc_id(), PackUtil.unpackMsg(taskInfo).get("msg"));
            List<ObjectTableBean> objectTableBeanList = objectCollectParamBean.getObjectTableBeanList();
            executor = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
            List<Future<JobStatusInfo>> list = new ArrayList<>();
            for (ObjectTableBean objectTableBean : objectTableBeanList) {
                objectTableBean.setEtlDate(etlDate);
                ObjectCollectParamBean objectCollectParamBean1 = new ObjectCollectParamBean();
                BeanUtil.copyProperties(objectCollectParamBean, objectCollectParamBean1);
                ObjectCollectJobImpl objectCollectJob = new ObjectCollectJobImpl(objectCollectParamBean1, objectTableBean);
                Future<JobStatusInfo> submit = executor.submit(objectCollectJob);
                list.add(submit);
            }
            JobStatusInfoUtil.printJobStatusInfo(list);
        } catch (Exception e) {
            log.error("执行对象采集入库任务失败:", e);
            message = "执行对象采集入库任务失败:" + e.getMessage();
        } finally {
            if (executor != null)
                executor.shutdown();
        }
        return message;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_path", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getDicTable(String file_path) {
        String xmlName = ConnUtil.getDataBaseFile("", "", file_path, "");
        Xls2xml.toXml2(file_path, xmlName);
        List<Map<String, String>> dicTable = ConnUtil.getDicTable(xmlName);
        return PackUtil.packMsg(JsonUtil.toJson(dicTable));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_path", desc = "", range = "")
    @Param(name = "data_date", desc = "", range = "", nullable = true)
    @Param(name = "file_suffix", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getFirstLineData(String file_path, String file_suffix, String data_date) {
        List<Map<String, String>> tableByNoDictionary = ConnUtil.getTableByNoDictionary(file_path, data_date, file_suffix);
        return PackUtil.packMsg(JsonUtil.toJson(tableByNoDictionary));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_path", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getAllDicColumns(String file_path) {
        String xmlName = ConnUtil.getDataBaseFile("", "", file_path, "");
        Xls2xml.toXml2(file_path, xmlName);
        return PackUtil.packMsg(JsonUtil.toJson(ConnUtil.getColumnByXml2(xmlName)));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_path", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getAllHandleType(String file_path) {
        String xmlName = ConnUtil.getDataBaseFile("", "", file_path, "");
        Xls2xml.toXml2(file_path, xmlName);
        return PackUtil.packMsg(JsonUtil.toJson(ConnUtil.getAllHandleType(xmlName)));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "objectCollectParam", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, String> getDictionaryParam(String jsonParam) {
        Map<String, String> unpackMsg = PackUtil.unpackMsg(jsonParam);
        return JsonUtil.toObject(unpackMsg.get("msg"), new TypeReference<Map<String, String>>() {
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dictionaryParam", desc = "", range = "")
    @Param(name = "file_path", desc = "", range = "")
    public void writeDictionary(String file_path, String dictionaryParam) {
        BufferedWriter bufferedWriter = null;
        try {
            Map<String, String> jsonMsgMap = getDictionaryParam(dictionaryParam);
            String dictionaryFilepath = file_path + File.separator + "writeDictionary" + File.separator;
            String jsonArray = jsonMsgMap.get("dictionaryParam");
            File dictionaryFile = new File(dictionaryFilepath);
            if (!dictionaryFile.exists()) {
                if (!dictionaryFile.mkdir()) {
                    throw new BusinessException("创建数据字典目录失败！");
                }
            }
            String pathName = dictionaryFilepath + DateUtil.getSysDate() + DateUtil.getSysTime() + "_dd_data.json";
            dictionaryFile = new File(pathName);
            bufferedWriter = new BufferedWriter(new FileWriter(dictionaryFile));
            bufferedWriter.write(jsonArray);
        } catch (Exception e) {
            throw new BusinessException("写dd_data.json时失败");
        } finally {
            if (bufferedWriter != null) {
                try {
                    bufferedWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
