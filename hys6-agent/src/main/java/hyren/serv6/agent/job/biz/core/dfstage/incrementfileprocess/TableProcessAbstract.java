package hyren.serv6.agent.job.biz.core.dfstage.incrementfileprocess;

import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.commons.utils.TypeTransLength;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.DataTypeConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class TableProcessAbstract implements TableProcessInterface {

    protected TableBean tableBean;

    protected CollectTableBean collectTableBean;

    protected List<String> dictionaryTypeList;

    protected List<String> dictionaryColumnList;

    protected Map<String, Boolean> isPrimaryKeyMap = new HashMap<>();

    protected List<String> insertColumnList;

    protected List<String> updateColumnList;

    protected List<String> deleteColumnList;

    protected TableProcessAbstract(TableBean tableBean, CollectTableBean collectTableBean) {
        this.collectTableBean = collectTableBean;
        this.tableBean = tableBean;
        this.dictionaryColumnList = StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT);
        this.dictionaryTypeList = StringUtil.split(tableBean.getAllType(), Constant.METAINFOSPLIT);
        this.insertColumnList = StringUtil.split(tableBean.getInsertColumnInfo().toUpperCase(), Constant.METAINFOSPLIT);
        this.updateColumnList = StringUtil.split(tableBean.getUpdateColumnInfo().toUpperCase(), Constant.METAINFOSPLIT);
        this.deleteColumnList = StringUtil.split(tableBean.getDeleteColumnInfo().toUpperCase(), Constant.METAINFOSPLIT);
        List<String> primaryKeyList = StringUtil.split(tableBean.getPrimaryKeyInfo(), Constant.METAINFOSPLIT);
        for (int i = 0; i < primaryKeyList.size(); i++) {
            this.isPrimaryKeyMap.put(dictionaryColumnList.get(i), IsFlag.Shi.getCode().equals(primaryKeyList.get(i)));
        }
    }

    @Override
    public void parserFileToTable(String readFile) {
        if (FileFormat.CSV.getCode().equals(tableBean.getFile_format())) {
            parseCsvFileToTable(readFile);
        } else if (FileFormat.DingChang.getCode().equals(tableBean.getFile_format())) {
            if (!StringUtil.isBlank(tableBean.getColumn_separator())) {
                parseNonFixedFileToTable(readFile);
            } else {
                parseFixedFileToTable(readFile);
            }
        } else if (FileFormat.FeiDingChang.getCode().equals(tableBean.getFile_format())) {
            parseNonFixedFileToTable(readFile);
        } else {
            throw new AppSystemException("db增量文件采集入库只支持定长、非定长、csv三种格式");
        }
    }

    private void parseFixedFileToTable(String readFile) {
        String lineValue;
        String code = DataBaseCode.ofValueByCode(tableBean.getFile_code());
        List<Integer> lengthList = new ArrayList<>();
        for (String type : dictionaryTypeList) {
            lengthList.add(TypeTransLength.getLength(type));
        }
        Map<String, Map<String, Object>> valueList;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(new File(readFile).toPath()), code))) {
            if (IsFlag.Shi.getCode().equals(tableBean.getIs_header())) {
                lineValue = br.readLine();
                if (lineValue != null) {
                    log.info("读取到表头为：" + lineValue);
                }
            }
            while ((lineValue = br.readLine()) != null) {
                valueList = getDingChangValueList(lineValue, dictionaryColumnList, lengthList, code);
                dealData(valueList);
            }
            excute();
        } catch (Exception e) {
            throw new AppSystemException("解析非定长文件转存报错", e);
        }
    }

    private void parseNonFixedFileToTable(String readFile) {
        String lineValue;
        String code = DataBaseCode.ofValueByCode(tableBean.getFile_code());
        Map<String, Map<String, Object>> valueList;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(new File(readFile).toPath()), code))) {
            if (IsFlag.Shi.getCode().equals(tableBean.getIs_header())) {
                lineValue = br.readLine();
                if (lineValue != null) {
                    log.info("读取到表头为：" + lineValue);
                }
            }
            while ((lineValue = br.readLine()) != null) {
                valueList = getFeiDingChangValueList(lineValue, dictionaryColumnList, tableBean.getColumn_separator());
                dealData(valueList);
            }
            excute();
        } catch (Exception e) {
            throw new AppSystemException("解析非定长文件转存报错", e);
        }
    }

    private void parseCsvFileToTable(String readFile) {
        List<String> lineList;
        String code = DataBaseCode.ofValueByCode(tableBean.getFile_code());
        Map<String, Map<String, Object>> valueList;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(readFile)), code));
            CsvListReader csvReader = new CsvListReader(reader, CsvPreference.EXCEL_PREFERENCE)) {
            if (IsFlag.Shi.getCode().equals(tableBean.getIs_header())) {
                lineList = csvReader.read();
                if (lineList != null) {
                    log.info("读取到表头为：" + JsonUtil.toJson(lineList));
                }
            }
            while ((lineList = csvReader.read()) != null) {
                valueList = getCsvValueList(lineList, dictionaryColumnList);
                dealData(valueList);
            }
            excute();
        } catch (Exception e) {
            throw new AppSystemException("解析非定长文件转存报错", e);
        }
    }

    private Map<String, Map<String, Object>> getDingChangValueList(String line, List<String> dictionaryColumnList, List<Integer> lengthList, String database_code) throws Exception {
        Map<String, Map<String, Object>> valueList = new HashMap<>();
        String operate = line.substring(0, 6);
        line = line.substring(6);
        Map<String, Object> map = new HashMap<>();
        byte[] bytes = line.getBytes(database_code);
        int begin = 0;
        for (int i = 0; i < dictionaryColumnList.size(); i++) {
            int length = lengthList.get(i);
            byte[] byteTmp = new byte[length];
            System.arraycopy(bytes, begin, byteTmp, 0, length);
            begin += length;
            String columnValue = new String(byteTmp, database_code);
            switch(operate) {
                case "insert":
                    if (insertColumnList.contains(dictionaryColumnList.get(i))) {
                        map.put(dictionaryColumnList.get(i), getColumnValue(dictionaryTypeList.get(i), columnValue));
                    }
                    break;
                case "update":
                    if (updateColumnList.contains(dictionaryColumnList.get(i))) {
                        map.put(dictionaryColumnList.get(i), getColumnValue(dictionaryTypeList.get(i), columnValue));
                    }
                    break;
                case "delete":
                    if (deleteColumnList.contains(dictionaryColumnList.get(i))) {
                        map.put(dictionaryColumnList.get(i), getColumnValue(dictionaryTypeList.get(i), columnValue));
                    }
                    break;
                default:
                    throw new AppSystemException("增量数据采集不自持" + operate + "操作");
            }
        }
        valueList.put(operate, map);
        return valueList;
    }

    private Map<String, Map<String, Object>> getFeiDingChangValueList(String line, List<String> dictionaryColumnList, String column_separator) {
        return getCsvValueList(StringUtil.split(line, column_separator), dictionaryColumnList);
    }

    private Map<String, Map<String, Object>> getCsvValueList(List<String> columnValueList, List<String> dictionaryColumnList) {
        Map<String, Map<String, Object>> valueList = new HashMap<>();
        String operate = columnValueList.get(0);
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < dictionaryColumnList.size(); i++) {
            switch(operate) {
                case "insert":
                    if (insertColumnList.contains(dictionaryColumnList.get(i))) {
                        map.put(dictionaryColumnList.get(i), getColumnValue(dictionaryTypeList.get(i), columnValueList.get(i + 1)));
                    }
                    break;
                case "update":
                    if (updateColumnList.contains(dictionaryColumnList.get(i))) {
                        map.put(dictionaryColumnList.get(i), getColumnValue(dictionaryTypeList.get(i), columnValueList.get(i + 1)));
                    }
                    break;
                case "delete":
                    if (deleteColumnList.contains(dictionaryColumnList.get(i))) {
                        map.put(dictionaryColumnList.get(i), getColumnValue(dictionaryTypeList.get(i), columnValueList.get(i + 1)));
                    }
                    break;
                default:
                    throw new AppSystemException("增量数据采集不自持" + operate + "操作");
            }
        }
        valueList.put(operate, map);
        return valueList;
    }

    private Object getColumnValue(String columnType, String columnValue) {
        Object str;
        columnType = columnType.toLowerCase();
        if (columnType.contains(DataTypeConstant.BOOLEAN.getMessage())) {
            str = Boolean.parseBoolean(columnValue.trim());
        } else if (columnType.contains(DataTypeConstant.INT8.getMessage()) || columnType.contains(DataTypeConstant.BIGINT.getMessage()) || columnType.contains(DataTypeConstant.LONG.getMessage())) {
            str = Long.parseLong(columnValue.trim());
        } else if (columnType.contains(DataTypeConstant.INT.getMessage())) {
            str = Integer.parseInt(columnValue.trim());
        } else if (columnType.contains(DataTypeConstant.FLOAT.getMessage())) {
            str = Float.parseFloat(columnValue.trim());
        } else if (columnType.contains(DataTypeConstant.DOUBLE.getMessage()) || columnType.contains(DataTypeConstant.DECIMAL.getMessage()) || columnType.contains(DataTypeConstant.NUMERIC.getMessage())) {
            str = Double.parseDouble(columnValue.trim());
        } else {
            if (columnValue == null) {
                str = "";
            } else {
                str = columnValue.trim();
            }
        }
        return str;
    }
}
