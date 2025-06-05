package hyren.serv6.agent.job.biz.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DataExtractUtil {

    private static final String DATADICTIONARY_SUFFIX = ".json";

    public static void writeDataDictionary(String dictionaryPath, String tableName, String table_ch_name, String allColumns, String allType, String allChColumns, String databaseType, List<DataExtractionDef> ext_defList, String unload_type, String primaryKeyInfo, String insertColumnInfo, String updateColumnInfo, String deleteColumnInfo, String hbase_name, String task_name) {
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        FileLock fileLock = null;
        try {
            randomAccessFile = new RandomAccessFile(new File(dictionaryPath + task_name + DATADICTIONARY_SUFFIX), "rw");
            fileChannel = randomAccessFile.getChannel();
            while (true) {
                try {
                    fileLock = fileChannel.tryLock();
                    break;
                } catch (Exception e) {
                    log.info("有其他线程正在操作该文件，当前线程休眠1秒");
                    TimeUnit.SECONDS.sleep(1);
                }
            }
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = randomAccessFile.readLine()) != null) {
                sb.append(new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
            }
            String dd_data = parseJsonDictionary(sb.toString().trim(), tableName, table_ch_name, allColumns, allChColumns, allType, databaseType, ext_defList, unload_type, primaryKeyInfo, insertColumnInfo, updateColumnInfo, deleteColumnInfo, hbase_name);
            randomAccessFile.setLength(0);
            randomAccessFile.write(dd_data.getBytes(StandardCharsets.ISO_8859_1));
        } catch (Exception e) {
            throw new AppSystemException("使用独占锁读取数据字典失败", e);
        } finally {
            try {
                if (fileLock != null)
                    fileLock.release();
                if (fileChannel != null)
                    fileChannel.close();
                if (randomAccessFile != null)
                    randomAccessFile.close();
            } catch (IOException e) {
                log.error("读取数据字典关闭流失败", e);
            }
        }
    }

    public static synchronized void writeSignalFile(String midName, String tableName, String sqlQuery, StringBuilder allColumns, StringBuilder allType, StringBuilder lengths, String is_fixed_extract, String fixed_separator, long lineCounter, long collect_database_size, String eltDate, String charset) {
        BufferedWriter bufferOutputWriter = null;
        OutputStreamWriter outputFileWriter = null;
        String create_date = DateUtil.getSysDate();
        String create_time = DateUtil.getSysTime();
        String signalFile = midName + ".flg";
        String fileName = tableName + "_" + eltDate + ".flg";
        try {
            File file = new File(signalFile);
            outputFileWriter = new OutputStreamWriter(Files.newOutputStream(file.toPath()), charset);
            bufferOutputWriter = new BufferedWriter(outputFileWriter, 4096);
            StringBuilder sb = new StringBuilder();
            sb.append(fileName).append(" ").append(collect_database_size).append(" ").append(lineCounter).append(" ").append(create_date).append(" ").append(create_time).append("\n\n");
            sb.append("FILENAME=").append(fileName).append("\n\n");
            sb.append("FILESIZE=").append(collect_database_size).append("\n\n");
            sb.append("ROWCOUNT=").append(lineCounter).append("\n\n");
            sb.append("CREATEDATETIME=").append(create_date).append(" ").append(create_time).append("\n\n");
            if (FileFormat.DingChang.getCode().equals(is_fixed_extract)) {
                sb.append("IS_FIXED_LENGTH=").append("YES").append("\n\n");
            } else {
                sb.append("IS_FIXED_LENGTH=").append("NO").append("\n\n");
            }
            sb.append("SEPARATOR=").append(fixed_separator).append("\n\n");
            sb.append("SQL=").append(sqlQuery).append("\n\n");
            int RowLength = 0;
            List<String> cols_length = StringUtil.split(lengths.toString(), "^");
            for (String length : cols_length) {
                RowLength += Integer.parseInt(length);
            }
            sb.append("ROWLENGTH=").append(RowLength).append("\n\n");
            sb.append("COLUMNCOUNT=").append(cols_length.size()).append("\n\n");
            sb.append("COLUMNDESCRIPTION=").append("\n");
            for (int i = 0; i < cols_length.size(); i++) {
                List<String> columns = StringUtil.split(allColumns.toString(), "^");
                List<String> types = StringUtil.split(allType.toString(), "^");
                if (StringUtil.isEmpty(fixed_separator)) {
                    int start = 0;
                    int end;
                    if (i > 0) {
                        for (int j = 0; j < i; j++) {
                            start += Integer.parseInt(cols_length.get(j));
                        }
                    }
                    start = start + 1;
                    end = start + Integer.parseInt(cols_length.get(i)) - 1;
                    sb.append(i + 1).append("$$").append(columns.get(i)).append("$$").append(types.get(i)).append("$$").append("(").append(start).append(",").append(end).append(")").append("\n");
                } else {
                    sb.append(i + 1).append("$$").append(columns.get(i)).append("$$").append(types.get(i)).append("\n");
                }
            }
            bufferOutputWriter.write(sb + "\n");
            bufferOutputWriter.flush();
        } catch (Exception e) {
            log.error("写信号文件失败", e);
        } finally {
            try {
                if (bufferOutputWriter != null)
                    bufferOutputWriter.close();
                if (outputFileWriter != null)
                    outputFileWriter.close();
            } catch (IOException e) {
                log.error("关闭流失败", e);
            }
        }
    }

    public static String parseJsonDictionary(String dd_data, String tableName, String table_ch_name, String allColumns, String allChColumns, String allType, String databaseType, List<DataExtractionDef> ext_defList, String unload_type, String primaryKeyInfo, String insertColumnInfo, String updateColumnInfo, String deleteColumnInfo, String hbase_name) {
        List<Map<String, Object>> jsonArray = new ArrayList<>();
        if (!StringUtil.isEmpty(dd_data)) {
            jsonArray = JsonUtil.toObject(dd_data, new TypeReference<List<Map<String, Object>>>() {
            });
            jsonArray.removeIf(jsonObject -> jsonObject.get("table_name").equals(tableName));
        }
        Map<String, Object> jsonObject = new HashMap<>();
        jsonObject.put("table_name", tableName);
        jsonObject.put("table_ch_name", StringUtil.isBlank(table_ch_name) ? tableName : table_ch_name);
        jsonObject.put("unload_type", unload_type);
        jsonObject.put("database_type", databaseType);
        jsonObject.put("insertColumnInfo", insertColumnInfo);
        jsonObject.put("updateColumnInfo", updateColumnInfo);
        jsonObject.put("deleteColumnInfo", deleteColumnInfo);
        List<Map<String, Object>> storageArray = new ArrayList<>();
        for (DataExtractionDef data_extraction_def : ext_defList) {
            Map<String, Object> object = new HashMap<>();
            object.put("is_header", data_extraction_def.getIs_header());
            object.put("dbfile_format", data_extraction_def.getDbfile_format());
            object.put("database_code", data_extraction_def.getDatabase_code());
            if (StringUtil.isEmpty(data_extraction_def.getFile_suffix())) {
                data_extraction_def.setFile_suffix("dat");
            }
            object.put("plane_url", data_extraction_def.getPlane_url() + File.separator + "#{date}" + File.separator + "#{table}" + File.separator + "#{file_format}" + File.separator + hbase_name + ".*." + data_extraction_def.getFile_suffix());
            if (StringUtil.isEmpty(data_extraction_def.getRow_separator())) {
                object.put("row_separator", "");
            } else {
                object.put("row_separator", StringUtil.string2Unicode(data_extraction_def.getRow_separator()));
            }
            if (StringUtil.isEmpty(data_extraction_def.getDatabase_separatorr())) {
                object.put("database_separatorr", "");
            } else {
                object.put("database_separatorr", StringUtil.string2Unicode(data_extraction_def.getDatabase_separatorr()));
            }
            storageArray.add(object);
        }
        jsonObject.put("storage", storageArray);
        List<String> columnList = StringUtil.split(allColumns, Constant.METAINFOSPLIT);
        List<String> typeList = StringUtil.split(allType, Constant.METAINFOSPLIT);
        List<String> primaryKeyList = StringUtil.split(primaryKeyInfo, Constant.METAINFOSPLIT);
        List<String> columnChList = StringUtil.split(allChColumns, Constant.METAINFOSPLIT);
        List<Map<String, Object>> array = new ArrayList<>();
        for (int i = 0; i < columnList.size(); i++) {
            Map<String, Object> object = new HashMap<>();
            object.put("column_type", typeList.get(i));
            object.put("column_remark", "");
            String column_name = columnList.get(i);
            object.put("column_name", column_name);
            String column_ch_name = "";
            if (!Constant.HYRENFIELD.contains(column_name.toUpperCase())) {
                column_ch_name = columnChList.get(i);
            }
            object.put("column_ch_name", StringUtil.isBlank(column_ch_name) ? column_name : column_ch_name);
            object.put("is_primary_key", primaryKeyList.get(i));
            object.put("is_get", IsFlag.Shi.getCode());
            object.put("is_alive", IsFlag.Shi.getCode());
            object.put("is_new", IsFlag.Fou.getCode());
            array.add(object);
        }
        jsonObject.put("columns", array);
        jsonArray.add(jsonObject);
        return JsonUtil.toJson(jsonArray);
    }
}
