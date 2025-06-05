package hyren.serv6.commons.dataclean;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.CharSplitType;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.FillingType;
import hyren.serv6.base.entity.ColumnSplit;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.writer.AbstractFileWriter;
import hyren.serv6.commons.utils.ColUtil;
import hyren.serv6.commons.utils.TypeTransLength;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Slf4j
public class DataClean_Biz implements DataCleanInterface {

    private final ColUtil cutil = new ColUtil();

    public String replace(Map<String, Map<String, String>> deleSpecialSpace, String columnData, String columnName) {
        if (deleSpecialSpace.size() != 0) {
            Map<String, String> colMap = deleSpecialSpace.get(columnName);
            if (null != colMap && colMap.size() != 0) {
                for (String key : colMap.keySet()) {
                    String str2 = colMap.get(key);
                    columnData = StringUtil.replace(columnData, key, str2);
                }
            }
        }
        return columnData;
    }

    public String filling(Map<String, String> strFilling, String columnData, String columnname) {
        if (strFilling.size() != 0) {
            String str = strFilling.get(columnname);
            if (!StringUtil.isEmpty(str)) {
                List<String> fi = StringUtil.split(str, Constant.METAINFOSPLIT);
                if (fi.size() == 3) {
                    int file_length = 0;
                    try {
                        file_length = Integer.parseInt(fi.get(0));
                    } catch (Exception e) {
                        log.error("字符补齐长度解析错误", e);
                    }
                    String filling_type = fi.get(1);
                    String character_filling = fi.get(2);
                    if (FillingType.QianBuQi.getCode().equals(filling_type)) {
                        columnData = StringUtils.leftPad(columnData, file_length, character_filling);
                    } else if (FillingType.HouBuQi.getCode().equals(filling_type)) {
                        columnData = StringUtils.rightPad(columnData, file_length, character_filling);
                    }
                }
            }
        }
        return columnData;
    }

    public String dateing(Map<String, String> dateing, String columnData, String columnName) {
        if (dateing.size() != 0) {
            String formatStr = dateing.get(columnName);
            if (!StringUtil.isEmpty(formatStr)) {
                if (!StringUtil.isEmpty(columnData)) {
                    try {
                        String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(formatStr, Constant.METAINFOSPLIT);
                        SimpleDateFormat newformat = new SimpleDateFormat(split[0]);
                        SimpleDateFormat oldformat = new SimpleDateFormat(split[1]);
                        Date parse = oldformat.parse(columnData);
                        columnData = newformat.format(parse);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                        throw new AppSystemException(columnName + "==" + columnData + "==" + formatStr + "列清洗日期转换失败");
                    }
                }
            }
        }
        return columnData;
    }

    public String split(Map<String, Map<String, ColumnSplit>> spliting, String columnData, String columnName, String type, String fileType, List<Object> list, String database_code, String database_separatorr) {
        if (spliting.get(columnName) != null && spliting.get(columnName).size() > 0) {
            StringBuilder sb = new StringBuilder(4096);
            if (FileFormat.CSV.getCode().equals(fileType)) {
                list.add(columnData);
                sb.append(columnData);
            } else if (FileFormat.SEQUENCEFILE.getCode().equals(fileType)) {
                sb.append(columnData).append(database_separatorr);
            } else if (FileFormat.ORC.getCode().equals(fileType)) {
                list.add(columnData);
                sb.append(columnData);
            } else if (FileFormat.FeiDingChang.getCode().equals(fileType)) {
                sb.append(columnData).append(database_separatorr);
            } else if (FileFormat.DingChang.getCode().equals(fileType)) {
                int length = TypeTransLength.getLength(type);
                String fixedData = AbstractFileWriter.columnToFixed(columnData, length, database_code, columnName);
                sb.append(fixedData).append(database_separatorr);
            } else {
                throw new AppSystemException("不支持的文件格式");
            }
            Map<String, ColumnSplit> colMap = spliting.get(columnName);
            ColumnSplit cp;
            if (null != colMap && colMap.size() != 0) {
                for (String colName : colMap.keySet()) {
                    cp = colMap.get(colName);
                    if (StringUtil.isEmpty(columnData)) {
                        if (FileFormat.CSV.getCode().equals(fileType)) {
                            list.add("");
                        } else if (FileFormat.SEQUENCEFILE.getCode().equals(fileType)) {
                            sb.append(database_separatorr);
                        } else if (FileFormat.ORC.getCode().equals(fileType)) {
                            list.add("");
                        } else if (FileFormat.FeiDingChang.getCode().equals(fileType)) {
                            sb.append(database_separatorr);
                        } else {
                            int length = TypeTransLength.getLength(cp.getCol_type());
                            String fixedData = AbstractFileWriter.columnToFixed(columnData, length, database_code, colName);
                            sb.append(fixedData).append(database_separatorr);
                        }
                    } else {
                        try {
                            String substr;
                            if (CharSplitType.PianYiLiang.getCode().equals(cp.getSplit_type())) {
                                String col_offset = cp.getCol_offset();
                                String[] split = col_offset.split(",");
                                int start = Integer.parseInt(split[0]);
                                int end = Integer.parseInt(split[1]);
                                substr = columnData.substring(start, end);
                            } else if (CharSplitType.ZhiDingFuHao.getCode().equals(cp.getSplit_type())) {
                                int num = cp.getSeq() == null ? 0 : Integer.parseInt(cp.getSeq().toString());
                                List<String> splitInNull = StringUtil.split(columnData, cp.getSplit_sep());
                                substr = splitInNull.get(num);
                            } else {
                                throw new AppSystemException("不支持的字符拆分方式");
                            }
                            if (FileFormat.CSV.getCode().equals(fileType)) {
                                list.add(substr);
                            } else if (FileFormat.SEQUENCEFILE.getCode().equals(fileType)) {
                                sb.append(substr).append(database_separatorr);
                            } else if (FileFormat.ORC.getCode().equals(fileType)) {
                                list.add(substr);
                            } else if (FileFormat.FeiDingChang.getCode().equals(fileType)) {
                                sb.append(substr).append(database_separatorr);
                            } else {
                                int length = TypeTransLength.getLength(cp.getCol_type());
                                String fixedData = AbstractFileWriter.columnToFixed(substr, length, database_code, colName);
                                sb.append(fixedData).append(database_separatorr);
                            }
                        } catch (Exception e) {
                            throw new AppSystemException("请检查" + colName + "字段定义的字符拆分的方式" + e.getMessage());
                        }
                    }
                }
            }
            if (database_separatorr.length() > 0 && sb.length() > database_separatorr.length()) {
                sb.delete(sb.length() - database_separatorr.length(), sb.length());
            }
            return sb.toString();
        } else {
            return columnData;
        }
    }

    public String codeTrans(Map<String, String> coding, String columnData, String columnName) {
        if (coding.size() != 0) {
            List<Map<String, Object>> jsonArray = JsonUtil.toObject(JsonUtil.toJson(coding.get(columnName)), new TypeReference<List<Map<String, Object>>>() {
            });
            if (jsonArray != null && jsonArray.size() > 0) {
                for (Map<String, Object> obj : jsonArray) {
                    if (obj != null && !obj.isEmpty()) {
                        if (columnData.equalsIgnoreCase(obj.get("orig_value").toString())) {
                            columnData = obj.get("code_value") == null ? columnData : obj.get("code_value").toString();
                        }
                    }
                }
            }
        }
        return columnData;
    }

    public String merge(Map<String, String> mergeing, String[] arrColString, String[] columns, List<Object> list, String fileType, String database_code, String database_separatorr) {
        StringBuilder return_sb = new StringBuilder(4096);
        if (mergeing.size() != 0) {
            for (String key : mergeing.keySet()) {
                StringBuilder sb = new StringBuilder();
                int[] index = findColIndex(columns, mergeing.get(key));
                for (int i : index) {
                    sb.append(arrColString[i]);
                }
                if (FileFormat.ORC.getCode().equals(fileType)) {
                    List<String> split = StringUtil.split(key, Constant.METAINFOSPLIT);
                    cutil.addData2Inspector(list, split.get(1).toUpperCase(), sb.toString());
                } else if (FileFormat.CSV.getCode().equals(fileType)) {
                    list.add(sb.toString());
                } else if (FileFormat.SEQUENCEFILE.getCode().equals(fileType)) {
                    return_sb.append(sb).append(database_separatorr);
                } else if (FileFormat.DingChang.getCode().equals(fileType)) {
                    List<String> split = StringUtil.split(key, Constant.METAINFOSPLIT);
                    int length = TypeTransLength.getLength(split.get(1));
                    String fixedStr = AbstractFileWriter.columnToFixed(sb.toString(), length, database_code, split.get(0).toUpperCase());
                    return_sb.append(fixedStr).append(database_separatorr);
                } else if (FileFormat.FeiDingChang.getCode().equals(fileType)) {
                    return_sb.append(sb.toString()).append(database_separatorr);
                } else {
                    throw new AppSystemException("不支持的文件格式");
                }
            }
            if (database_separatorr.length() > 0 && return_sb.length() > database_separatorr.length()) {
                return_sb.delete(return_sb.length() - database_separatorr.length(), return_sb.length());
            }
        }
        return return_sb.toString();
    }

    public String trim(Map<String, String> Triming, String columnData, String columnName) {
        if (!Triming.isEmpty()) {
            String colMap = Triming.get(columnName);
            if (!StringUtil.isEmpty(colMap)) {
                if (!StringUtil.isEmpty(columnData)) {
                    columnData = columnData.trim();
                } else {
                    columnData = "";
                }
            }
        }
        return columnData;
    }

    private int[] findColIndex(String[] column, String str) {
        List<String> split = StringUtil.split(str, ",");
        int[] index = new int[split.size()];
        for (int i = 0; i < split.size(); i++) {
            for (int j = 0; j < column.length; j++) {
                if (split.get(i).equalsIgnoreCase(column[j])) {
                    index[i] = j;
                }
            }
        }
        return index;
    }
}
