package hyren.serv6.g.commons;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.base.utils.ActionResult;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.g.enumerate.AsynType;
import hyren.serv6.g.enumerate.DataType;
import hyren.serv6.g.enumerate.OutType;
import hyren.serv6.g.enumerate.StateType;
import hyren.serv6.g.serviceuser.common.InterfaceCommon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class LocalFile {

    private static final Logger logger = LogManager.getLogger();

    @Method(desc = "", logicStep = "")
    @Param(name = "feedback", desc = "", range = "")
    @Param(name = "dataType", desc = "", range = "")
    @Param(name = "outType", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult dealDataByType(DatabaseWrapper db, ActionResult feedback, String dataType, String outType, Long user_id) {
        try {
            if (!DataType.isDataType(dataType)) {
                return StateType.getActionResult(StateType.DATA_TYPE_ERROR);
            }
            if (!OutType.isOutType(outType)) {
                return StateType.getActionResult(StateType.OUT_TYPE_ERROR);
            }
            if (OutType.STREAM == OutType.ofEnumByCode(outType)) {
                if (DataType.json == DataType.ofEnumByCode(dataType)) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("dataType", dataType);
                    map.put("outType", outType);
                    map.put("data", feedback.getData());
                    feedback.setData(map);
                } else {
                    csvData(feedback);
                }
            } else {
                String uuid = UUID.randomUUID().toString();
                File filePath = createFile(uuid, dataType);
                boolean isWriteSuccess = writeDataFile(filePath, feedback, dataType);
                if (isWriteSuccess) {
                    if (InterfaceCommon.saveFileInfo(db, user_id, uuid, dataType, outType, CommonVariables.RESTFILEPATH) == 1) {
                        Map<String, Object> obj = new HashMap<String, Object>();
                        obj.put("uuid", uuid);
                        obj.put("dataType", dataType);
                        obj.put("outType", outType);
                        feedback.setData(obj);
                    } else {
                        feedback = StateType.getActionResult(StateType.EXCEPTION);
                        feedback.setData("保存接口文件信息失败");
                    }
                } else {
                    logger.info("数据输出形式为 : " + outType + ",删除文件" + filePath);
                    FileUtil.deleteDirectory(filePath);
                }
            }
            return feedback;
        } catch (IOException e) {
            logger.error(e);
            return StateType.getActionResult(StateType.EXCEPTION);
        }
    }

    public static void dealDataByAsynType(String outType, String asynType, String backUrl, String filePath, String fileName, ActionResult actionResult) {
        if (OutType.FILE == OutType.ofEnumByCode(outType)) {
            if (AsynType.ASYNCALLBACK == AsynType.ofEnumByCode(asynType)) {
                InterfaceCommon.checkBackUrl(actionResult, backUrl);
            } else if (AsynType.ASYNPOLLING == AsynType.ofEnumByCode(asynType)) {
                InterfaceCommon.createFile(actionResult, filePath, fileName);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "uuid", desc = "", range = "")
    @Param(name = "dataType", desc = "", range = "")
    @Return(desc = "", range = "")
    public static File createFile(String uuid, String dataType) {
        String filePath = CommonVariables.RESTFILEPATH + File.separator + uuid + '.' + dataType;
        File writeFile;
        try {
            File file = new File(CommonVariables.RESTFILEPATH);
            if (!file.exists() && !file.isDirectory()) {
                if (!file.mkdirs()) {
                    throw new AppSystemException("创建目录失败:" + file.getAbsolutePath());
                }
            }
            writeFile = new File(filePath);
            if (!writeFile.exists()) {
                if (!writeFile.createNewFile()) {
                    throw new AppSystemException("创建文件失败:" + writeFile.getAbsolutePath());
                }
            }
        } catch (IOException e) {
            throw new AppSystemException(e.getMessage());
        }
        return writeFile;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Param(name = "feedback", desc = "", range = "")
    @Param(name = "dataType", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean writeDataFile(File file, ActionResult feedback, String dataType) {
        BufferedWriter writer;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JavaType javaType = objectMapper.getTypeFactory().constructCollectionType(List.class, objectMapper.getTypeFactory().constructMapType(Map.class, String.class, String.class));
            List<Map<String, String>> listData = objectMapper.readValue(feedback.getData().toString(), javaType);
            writer = new BufferedWriter(new FileWriter(file));
            List<String> columnList = new ArrayList<>();
            int size = 0;
            if (DataType.csv == DataType.ofEnumByCode(dataType)) {
                for (Map<String, String> map : listData) {
                    StringBuilder builder = new StringBuilder();
                    for (Entry<String, String> entry : map.entrySet()) {
                        builder.append(entry.getValue()).append(',');
                        String column = entry.getKey().toLowerCase();
                        if (!columnList.contains(column)) {
                            columnList.add(column);
                        }
                    }
                    if (size == 0) {
                        writer.write(StringUtil.join(columnList, ","));
                        writer.newLine();
                    }
                    String columnData = builder.deleteCharAt(builder.length() - 1).toString();
                    writer.write(columnData);
                    writer.newLine();
                    if (size % 100000 == 0) {
                        writer.flush();
                    }
                    size++;
                }
            } else {
                for (Map<String, String> map : listData) {
                    Map<String, Object> jsonDataObj = new HashMap<String, Object>();
                    for (Entry<String, String> entry : map.entrySet()) {
                        jsonDataObj.put(entry.getKey().toLowerCase(), entry.getValue());
                    }
                    writer.write(JsonUtil.toJson(jsonDataObj));
                    writer.newLine();
                    if (size % 100000 == 0) {
                        writer.flush();
                    }
                    size++;
                }
            }
            writer.flush();
            writer.close();
            return true;
        } catch (IOException e) {
            logger.error(e);
        }
        return false;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "feedback", desc = "", range = "")
    public static void csvData(ActionResult feedback) {
        try {
            List<Map<String, String>> listData = JsonUtil.toObject(feedback.getData().toString(), List.class);
            List<String> columnList = new ArrayList<>();
            List<String> columnData = new ArrayList<>();
            for (Map<String, String> map : listData) {
                StringBuilder builder = new StringBuilder();
                for (Entry<String, String> entry : map.entrySet()) {
                    String column = entry.getKey().toLowerCase();
                    builder.append(entry.getValue()).append(',');
                    if (!columnList.contains(column)) {
                        columnList.add(column);
                    }
                }
                builder.deleteCharAt(builder.length() - 1);
                columnData.add(builder.toString());
            }
            Map<String, Object> obj = new HashMap<>();
            obj.put("column", String.join(",", columnList));
            obj.put("data", columnData);
            feedback.setData(obj);
        } catch (Exception e) {
            feedback.setData("流数据量过大...请使用文件:" + e.getMessage());
        }
    }
}
