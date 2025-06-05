package hyren.serv6.commons.utils.xlstoxml.util;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ColumnMeta {

    public static List<String> getColumnListByDictionary(String tableName, String dictionary_file_path) {
        if (dictionary_file_path.endsWith("json")) {
            return getColumnListByJson(tableName, dictionary_file_path);
        } else if (dictionary_file_path.endsWith("xls") || dictionary_file_path.endsWith("xlsx")) {
            return getColumnListByExcel(tableName, dictionary_file_path);
        } else {
            throw new AppSystemException("数据字典的文件格式不正确");
        }
    }

    public static List<String> getIncrementColumnListByDictionary(String tableName, String dictionary_file_path) {
        if (dictionary_file_path.endsWith("json")) {
            return getIncrementColumnListByJson(tableName, dictionary_file_path);
        } else if (dictionary_file_path.endsWith("xls") || dictionary_file_path.endsWith("xlsx")) {
            return getIncrementColumnListByExcel(tableName, dictionary_file_path);
        } else {
            throw new AppSystemException("数据字典的文件格式不正确");
        }
    }

    private static List<String> getColumnListByExcel(String tableName, String excel_file_path) {
        log.info("getColumnListByExcel: " + tableName + "-----" + excel_file_path);
        List<String> cList = new ArrayList<>();
        String xmlName = ConnUtil.getDataBaseFile("", "", excel_file_path, "");
        Map<String, List<Map<String, String>>> columnList = ConnUtil.getColumnByXml(xmlName);
        columnList.forEach((itemTableName, columnMap) -> {
            if (itemTableName.equalsIgnoreCase(tableName)) {
                columnMap.forEach(columnInfo -> {
                    cList.add(columnInfo.get("column_name") + Constant.METAINFOSPLIT + columnInfo.get("column_type") + Constant.METAINFOSPLIT + columnInfo.get("is_primary_key") + Constant.METAINFOSPLIT + columnInfo.get("column_tar_type"));
                });
            }
        });
        log.info("表: " + tableName + " 的列信息是: " + cList);
        return cList;
    }

    private static List<String> getIncrementColumnListByExcel(String tableName, String excel_file_path) {
        log.info(tableName + "-----" + excel_file_path);
        return new ArrayList<>();
    }

    private static List<String> getColumnListByJson(String tableName, String json_file_path) {
        List<String> cList = new ArrayList<>();
        try {
            String dd_data = FileUtils.readFileToString(new File(json_file_path), StandardCharsets.UTF_8);
            List<Map<String, Object>> tableMetaArray = JsonUtil.toObject(dd_data, new TypeReference<List<Map<String, Object>>>() {
            });
            for (Map<String, Object> tableMeta : tableMetaArray) {
                if (tableName.equalsIgnoreCase(tableMeta.get("table_name").toString())) {
                    List<Map<String, Object>> columnMetaArray = JsonUtil.toObject(JsonUtil.toJson(tableMeta.get("columns")), new TypeReference<List<Map<String, Object>>>() {
                    });
                    for (Map<String, Object> columnMeta : columnMetaArray) {
                        cList.add(columnMeta.get("column_name") + Constant.METAINFOSPLIT + columnMeta.get("column_type") + Constant.METAINFOSPLIT + columnMeta.get("is_primary_key") + Constant.METAINFOSPLIT + columnMeta.get("column_tar_type"));
                    }
                }
            }
        } catch (Exception e) {
            throw new AppSystemException("获取字段信息异常", e);
        }
        return cList;
    }

    private static List<String> getIncrementColumnListByJson(String tableName, String json_file_path) {
        List<String> incrementColumnList = new ArrayList<>();
        try {
            String dd_data = FileUtils.readFileToString(new File(json_file_path), StandardCharsets.UTF_8);
            List<Map<String, Object>> tableMetaArray = JsonUtil.toObject(dd_data, new TypeReference<List<Map<String, Object>>>() {
            });
            for (Map<String, Object> tableMeta : tableMetaArray) {
                if (tableName.equalsIgnoreCase(tableMeta.get("table_name").toString())) {
                    incrementColumnList.add(tableMeta.get("insertColumnInfo").toString());
                    incrementColumnList.add(tableMeta.get("updateColumnInfo").toString());
                    incrementColumnList.add(tableMeta.get("deleteColumnInfo").toString());
                }
            }
        } catch (Exception e) {
            throw new AppSystemException("获取数据字典中的新增、更新、删除字段解析异常", e);
        }
        return incrementColumnList;
    }
}
