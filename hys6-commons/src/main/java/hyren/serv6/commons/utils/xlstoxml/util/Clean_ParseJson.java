package hyren.serv6.commons.utils.xlstoxml.util;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.entity.ColumnSplit;
import java.util.*;

public class Clean_ParseJson {

    public static String STRSPLIT = "^";

    public static Map<String, Object> parseJson(Map<String, Object> jsonsingle, Map<String, Object> jsonmsg, String columns) throws Exception {
        Map<String, Object> all = new HashMap<String, Object>();
        Map<String, Map<String, String>> deleSpecialSpace = new HashMap<String, Map<String, String>>();
        Map<String, String> strFilling = new HashMap<String, String>();
        Map<String, String> strDateing = new HashMap<String, String>();
        Map<String, Map<String, ColumnSplit>> splitIng = new HashMap<String, Map<String, ColumnSplit>>();
        Map<String, String> mergeIng = new LinkedHashMap<String, String>();
        Map<String, String> codeIng = new HashMap<String, String>();
        Map<String, String> Triming = new HashMap<String, String>();
        Map<String, Map<Integer, String>> ordering = new HashMap<String, Map<Integer, String>>();
        String column_file_length = "0";
        Object allColumnCleanResult = jsonsingle.get("all_column_clean_result");
        if (null != allColumnCleanResult) {
            Map<String, Object> all_column_clean_result = JsonUtil.toObject(JsonUtil.toJson(allColumnCleanResult), new TypeReference<Map<String, Object>>() {
            });
            List<String> split = StringUtil.split(columns.toLowerCase(), ",");
            for (String columnName : split) {
                Map<String, Object> column_name = JsonUtil.toObject(JsonUtil.toJson(all_column_clean_result.get(columnName)), new TypeReference<Map<String, Object>>() {
                });
                String order = column_name.get("clean_order").toString();
                Map<Integer, String> changeKeyValue = changeKeyValue(order);
                Object isColumnRepeatResult = column_name.get("is_column_repeat_result");
                List<Map<String, Object>> is_column_repeat_result = JsonUtil.toObject(JsonUtil.toJson(isColumnRepeatResult), new TypeReference<List<Map<String, Object>>>() {
                });
                Map<String, String> colmap = new HashMap<String, String>();
                if (!is_column_repeat_result.isEmpty()) {
                    for (Map<String, Object> stringObjectMap : is_column_repeat_result) {
                        Map<String, Object> object = JsonUtil.toObject(JsonUtil.toJson(stringObjectMap), new TypeReference<Map<String, Object>>() {
                        });
                        String field = StringUtil.unicode2String(object.get("field").toString());
                        String replace_feild = StringUtil.unicode2String(object.get("replace_feild").toString());
                        colmap.put(field, replace_feild);
                    }
                }
                deleSpecialSpace.put(columnName.toUpperCase(), colmap);
                StringBuffer filling = new StringBuffer();
                Map<String, Object> is_column_file_result = JsonUtil.toObject(JsonUtil.toJson(column_name.get("is_column_file_result")), new TypeReference<Map<String, Object>>() {
                });
                if (!is_column_file_result.isEmpty()) {
                    column_file_length = is_column_file_result.get("filling_length").toString();
                    String filling_type = is_column_file_result.get("filling_type").toString();
                    String character_filling = StringUtil.unicode2String(is_column_file_result.get("character_filling").toString());
                    filling.append(column_file_length).append(STRSPLIT).append(filling_type).append(STRSPLIT).append(character_filling);
                }
                strFilling.put(columnName.toUpperCase(), filling.toString());
                StringBuffer dateing = new StringBuffer();
                Map<String, Object> is_column_time_result = JsonUtil.toObject(JsonUtil.toJson(column_name.get("is_column_time_result")), new TypeReference<Map<String, Object>>() {
                });
                if (!is_column_time_result.isEmpty()) {
                    String convert_format = (is_column_time_result.get("convert_format").toString());
                    String old_format = (is_column_time_result.get("old_format").toString());
                    dateing.append(convert_format).append(STRSPLIT).append(old_format);
                }
                strDateing.put(columnName.toUpperCase(), dateing.toString());
                Map<String, ColumnSplit> splitmap = new LinkedHashMap<String, ColumnSplit>();
                List<Map<String, Object>> columnSplitResult = JsonUtil.toObject(JsonUtil.toJson(column_name.get("columnSplitResult")), new TypeReference<List<Map<String, Object>>>() {
                });
                if (!columnSplitResult.isEmpty()) {
                    for (Map<String, Object> stringObjectMap : columnSplitResult) {
                        String split_sep = StringUtil.unicode2String(stringObjectMap.get("split_sep").toString());
                        ColumnSplit cp = new ColumnSplit();
                        cp.setCol_offset(stringObjectMap.get("col_offset").toString());
                        cp.setSplit_sep(split_sep);
                        cp.setSeq(Long.parseLong(stringObjectMap.get("seq").toString()));
                        cp.setSplit_type(stringObjectMap.get("split_type").toString());
                        cp.setCol_type(stringObjectMap.get("col_type").toString());
                        splitmap.put(stringObjectMap.get("col_name").toString(), cp);
                    }
                    splitIng.put(columnName.toUpperCase(), splitmap);
                }
                Map<String, Object> columnCodeResult = JsonUtil.toObject(JsonUtil.toJson(column_name.get("columnCodeResult")), new TypeReference<Map<String, Object>>() {
                });
                if (!columnCodeResult.isEmpty()) {
                    codeIng.put(columnName.toUpperCase(), columnCodeResult.toString());
                }
                Map<String, Object> trimResult = JsonUtil.toObject(JsonUtil.toJson(column_name.get("trimResult")), new TypeReference<Map<String, Object>>() {
                });
                if (!trimResult.isEmpty()) {
                    Triming.put(columnName.toUpperCase(), trimResult.toString());
                }
                if (!changeKeyValue.isEmpty()) {
                    ordering.put(columnName.toUpperCase(), changeKeyValue);
                }
            }
            Map<String, Object> table_clean_result = JsonUtil.toObject(JsonUtil.toJson(jsonsingle.get("table_clean_result")), new TypeReference<Map<String, Object>>() {
            });
            String order = table_clean_result.get("clean_order").toString();
            Map<Integer, String> changeKeyValue = changeKeyValue(order);
            List<Map<String, Object>> is_table_repeat_result = JsonUtil.toObject(JsonUtil.toJson(table_clean_result.get("is_table_repeat_result")), new TypeReference<List<Map<String, Object>>>() {
            });
            if (!is_table_repeat_result.isEmpty()) {
                Map<String, String> tablemap = new HashMap<String, String>();
                for (Map<String, Object> stringObjectMap : is_table_repeat_result) {
                    String field = StringUtil.unicode2String(stringObjectMap.get("field").toString());
                    String replace_feild = StringUtil.unicode2String(stringObjectMap.get("replace_feild").toString());
                    tablemap.put(field, replace_feild);
                }
                deleSpecialSpace.put("tablemap_deleSpecialSpace", tablemap);
            }
            Map<String, Object> is_table_fille_result = JsonUtil.toObject(JsonUtil.toJson(table_clean_result.get("is_table_fille_result")), new TypeReference<Map<String, Object>>() {
            });
            StringBuffer filling = new StringBuffer();
            if (!is_table_fille_result.isEmpty()) {
                column_file_length = is_table_fille_result.get("filling_length").toString();
                String filling_type = is_table_fille_result.get("filling_type").toString();
                String character_filling = StringUtil.unicode2String(is_table_fille_result.get("character_filling").toString());
                filling.append(column_file_length).append(STRSPLIT).append(filling_type).append(STRSPLIT).append(character_filling);
                strFilling.put("tablemap_strfilling", filling.toString());
            }
            Map<String, Object> isTableTrimResult = JsonUtil.toObject(JsonUtil.toJson(table_clean_result.get("isTableTrimResult")), new TypeReference<Map<String, Object>>() {
            });
            if (!isTableTrimResult.isEmpty()) {
                Triming.put("tablemap_striming", isTableTrimResult.toString());
            }
            List<Map<String, Object>> columnMergeResult = JsonUtil.toObject(JsonUtil.toJson(table_clean_result.get("columnMergeResult")), new TypeReference<List<Map<String, Object>>>() {
            });
            if (!columnMergeResult.isEmpty()) {
                for (Map<String, Object> stringObjectMap : columnMergeResult) {
                    mergeIng.put(stringObjectMap.get("col_name") + STRSPLIT + stringObjectMap.get("col_type"), stringObjectMap.get("old_name").toString());
                }
            }
            if (!changeKeyValue.isEmpty()) {
                ordering.put("tablemap", changeKeyValue);
            }
        }
        Map<String, Object> all_clean_result = JsonUtil.toObject(JsonUtil.toJson(jsonmsg.get("all_clean_result")), new TypeReference<Map<String, Object>>() {
        });
        List<Map<String, Object>> is_all_repeat_result = JsonUtil.toObject(JsonUtil.toJson(all_clean_result.get("is_all_repeat_result")), new TypeReference<List<Map<String, Object>>>() {
        });
        String order = all_clean_result.get("clean_order").toString();
        Map<Integer, String> changeKeyValue = changeKeyValue(order);
        if (!is_all_repeat_result.isEmpty()) {
            Map<String, String> taskmap = new HashMap<String, String>();
            for (Map<String, Object> stringObjectMap : is_all_repeat_result) {
                String field = StringUtil.unicode2String(stringObjectMap.get("field").toString());
                String replace_feild = StringUtil.unicode2String(stringObjectMap.get("replace_feild").toString());
                taskmap.put(field, replace_feild);
            }
            deleSpecialSpace.put("taskmap_deleSpecialSpace", taskmap);
        }
        Map<String, Object> is_all_fille_result = JsonUtil.toObject(JsonUtil.toJson(all_clean_result.get("is_all_fille_result")), new TypeReference<Map<String, Object>>() {
        });
        StringBuffer filling = new StringBuffer();
        if (!is_all_fille_result.isEmpty()) {
            column_file_length = is_all_fille_result.get("filling_length").toString();
            String filling_type = is_all_fille_result.get("filling_type").toString();
            String character_filling = StringUtil.unicode2String(is_all_fille_result.get("character_filling").toString());
            filling.append(column_file_length).append(STRSPLIT).append(filling_type).append(STRSPLIT).append(character_filling);
        }
        strFilling.put("taskmap_strfilling", filling.toString());
        Map<String, Object> isAllTrimResult = JsonUtil.toObject(JsonUtil.toJson(all_clean_result.get("isAllTrimResult")), new TypeReference<Map<String, Object>>() {
        });
        if (!isAllTrimResult.isEmpty()) {
            Triming.put("taskmap_striming", isAllTrimResult.toString());
        }
        if (!changeKeyValue.isEmpty()) {
            ordering.put("taskmap", changeKeyValue);
        }
        all.put("deleSpecialSpace", deleSpecialSpace);
        all.put("strFilling", strFilling);
        all.put("dating", strDateing);
        all.put("splitIng", splitIng);
        all.put("mergeIng", mergeIng);
        all.put("codeIng", codeIng);
        all.put("Triming", Triming);
        all.put("ordering", ordering);
        return all;
    }

    private static Map<Integer, String> changeKeyValue(String order) {
        Map<Integer, String> map = new HashMap<>();
        if (!StringUtil.isEmpty(order)) {
            Map<String, Object> jsonOrder = JsonUtil.toObject(JsonUtil.toJson(order), new TypeReference<Map<String, Object>>() {
            });
            Set<String> jsonSet = jsonOrder.keySet();
            for (String key : jsonSet) {
                map.put(Integer.parseInt(jsonOrder.get(key).toString()), key);
            }
        }
        return map;
    }
}
