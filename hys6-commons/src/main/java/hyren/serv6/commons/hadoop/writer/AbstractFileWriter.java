package hyren.serv6.commons.hadoop.writer;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.MD5Util;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.*;

@DocClass(desc = "", author = "WangZhengcheng")
@Slf4j
public abstract class AbstractFileWriter implements FileWriterInterface {

    private static final String SCHEMA_JSON = "{\"type\": \"record\",\"name\": \"BigFilesTest\", " + "\"fields\": [" + "{\"name\":\"" + "currValue" + "\",\"type\":\"string\"}," + "{\"name\":\"" + "readerToByte" + "\", \"type\":\"bytes\"}" + "]}";

    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    private static final GenericRecord record = new GenericData.Record(SCHEMA);

    protected ResultSet resultSet;

    protected CollectTableBean collectTableBean;

    protected int pageNum;

    protected TableBean tableBean;

    protected DataExtractionDef dataExtractionDef;

    protected Dbtype db_type;

    protected String operateDate;

    protected String operateTime;

    protected Long user_id;

    public AbstractFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef) {
        this.resultSet = resultSet;
        this.collectTableBean = collectTableBean;
        this.pageNum = pageNum;
        this.tableBean = tableBean;
        this.dataExtractionDef = dataExtractionDef;
        this.operateDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        this.operateTime = new SimpleDateFormat("HH:mm:ss").format(new Date());
        this.user_id = collectTableBean.getUser_id();
        this.db_type = collectTableBean.getDb_type();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "blob", desc = "", range = "")
    @Return(desc = "", range = "")
    private byte[] blobToBytes(Blob blob) {
        BufferedInputStream is = null;
        try {
            is = new BufferedInputStream(blob.getBinaryStream());
            byte[] bytes = new byte[(int) blob.length()];
            int len = bytes.length;
            int offset = 0;
            int read;
            while (offset < len && (read = is.read(bytes, offset, len - offset)) >= 0) {
                offset += read;
            }
            return bytes;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new AppSystemException("jdbc获取blob的数据转为byte异常");
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    protected String getOneColumnValue(DataFileWriter<Object> avroWriter, long lineCounter, int pageNum, ResultSet resultSet, int type, StringBuilder sb_, String column_name, String hbase_name, String midName) throws SQLException, IOException {
        String lobs_file_name = "";
        String reader2String = "";
        byte[] readerToByte = null;
        if (type == Types.BLOB || type == Types.LONGVARBINARY) {
            Blob blob = resultSet.getBlob(column_name);
            if (null != blob) {
                readerToByte = blobToBytes(blob);
                if (readerToByte.length > 0) {
                    lobs_file_name = "LOBs_" + hbase_name + "_" + column_name + "_" + pageNum + "_" + lineCounter + "_BLOB_" + avroWriter.sync();
                    sb_.append(lobs_file_name);
                    reader2String = new String(readerToByte);
                }
            }
        } else if (type == Types.VARBINARY) {
            InputStream binaryStream = resultSet.getBinaryStream(column_name);
            if (null != binaryStream) {
                readerToByte = IOUtils.toByteArray(binaryStream);
                if (readerToByte != null && readerToByte.length > 0) {
                    lobs_file_name = "LOBs_" + hbase_name + "_" + column_name + "_" + pageNum + "_" + lineCounter + "_VARBINARY_" + avroWriter.sync();
                    sb_.append(lobs_file_name);
                    reader2String = new String(readerToByte);
                }
            }
        } else if (type == Types.CLOB) {
            Object obj = resultSet.getClob(column_name);
            if (null != obj) {
                Reader characterStream = resultSet.getClob(column_name).getCharacterStream();
                reader2String = readerStreamToString(characterStream);
                reader2String = clearIrregularData(reader2String);
            }
            sb_.append(reader2String);
        } else {
            Object oj = resultSet.getObject(column_name);
            if (null != oj) {
                if (type == Types.TIMESTAMP) {
                    String dateStr = resultSet.getTimestamp(column_name).toString();
                    if (dateStr.endsWith(".0")) {
                        dateStr = dateStr.replace(".0", "");
                    }
                    reader2String = dateStr;
                } else if (type == Types.DATE) {
                    Date date = resultSet.getDate(column_name);
                    reader2String = date.toString();
                } else if (type == Types.TIME) {
                    Date date = resultSet.getTime(column_name);
                    reader2String = date.toString();
                } else if (type == Types.CHAR || type == Types.VARCHAR || type == Types.NVARCHAR || type == Types.BINARY || type == Types.LONGVARCHAR) {
                    reader2String = oj.toString();
                } else {
                    reader2String = oj.toString();
                }
                reader2String = clearIrregularData(reader2String);
            }
            sb_.append(reader2String);
        }
        if (readerToByte != null && readerToByte.length > 0) {
            record.put("currValue", lobs_file_name);
            record.put("readerToByte", ByteBuffer.wrap(readerToByte));
            avroWriter.append(record);
            writeLobsFileToOracle(midName + "LOBS", lobs_file_name, readerToByte);
        }
        return reader2String;
    }

    protected String getOneColumnValue(DataFileWriter<Object> avroWriter, long lineCounter, int pageNum, ResultSet resultSet, int type, StringBuilder sb_, String column_name, String hbase_name, String midName, int i) throws SQLException, IOException {
        String lobs_file_name = "";
        String reader2String = "";
        byte[] readerToByte = null;
        if (type == Types.BLOB || type == Types.LONGVARBINARY) {
            Blob blob = resultSet.getBlob(i);
            if (null != blob) {
                readerToByte = blobToBytes(blob);
                if (readerToByte.length > 0) {
                    lobs_file_name = "LOBs_" + hbase_name + "_" + column_name + "_" + pageNum + "_" + lineCounter + "_BLOB_" + avroWriter.sync();
                    sb_.append(lobs_file_name);
                    reader2String = new String(readerToByte);
                }
            }
        } else if (type == Types.VARBINARY) {
            InputStream binaryStream = resultSet.getBinaryStream(i);
            if (null != binaryStream) {
                readerToByte = IOUtils.toByteArray(binaryStream);
                if (readerToByte != null && readerToByte.length > 0) {
                    lobs_file_name = "LOBs_" + hbase_name + "_" + column_name + "_" + pageNum + "_" + lineCounter + "_VARBINARY_" + avroWriter.sync();
                    sb_.append(lobs_file_name);
                    reader2String = new String(readerToByte);
                }
            }
        } else if (type == Types.CLOB) {
            Object obj = resultSet.getClob(i);
            if (null != obj) {
                Reader characterStream = resultSet.getClob(i).getCharacterStream();
                reader2String = readerStreamToString(characterStream);
                reader2String = clearIrregularData(reader2String);
            }
            sb_.append(reader2String);
        } else {
            Object oj = resultSet.getObject(i);
            if (null != oj) {
                if (type == Types.TIMESTAMP) {
                    String dateStr = resultSet.getTimestamp(i).toString();
                    if (dateStr.endsWith(".0")) {
                        dateStr = dateStr.replace(".0", "");
                    }
                    reader2String = dateStr;
                } else if (type == Types.DATE) {
                    Date date = resultSet.getDate(i);
                    reader2String = date.toString();
                } else if (type == Types.TIME) {
                    Date date = resultSet.getTime(i);
                    reader2String = date.toString();
                } else if (type == Types.CHAR || type == Types.VARCHAR || type == Types.NVARCHAR || type == Types.BINARY || type == Types.LONGVARCHAR) {
                    reader2String = oj.toString();
                } else {
                    reader2String = oj.toString();
                }
                reader2String = clearIrregularData(reader2String);
            }
            sb_.append(reader2String);
        }
        if (readerToByte != null && readerToByte.length > 0) {
            record.put("currValue", lobs_file_name);
            record.put("readerToByte", ByteBuffer.wrap(readerToByte));
            avroWriter.append(record);
            writeLobsFileToOracle(midName + "LOBS", lobs_file_name, readerToByte);
        }
        return reader2String;
    }

    public static String clearIrregularData(String columnData) {
        if (columnData.contains("\r")) {
            columnData = columnData.replace('\r', ' ');
        }
        if (columnData.contains("\n")) {
            columnData = columnData.replace('\n', ' ');
        }
        if (columnData.contains("\r\n")) {
            columnData = StringUtil.replace(columnData, "\r\n", " ");
        }
        return columnData;
    }

    private String readerStreamToString(Reader characterStream) throws IOException {
        BufferedReader br = new BufferedReader(characterStream);
        String s;
        StringBuilder sb = new StringBuilder();
        while ((s = br.readLine()) != null) {
            sb.append(s);
        }
        return sb.toString();
    }

    private void writeLobsFileToOracle(String lobs_path, String lobs_file_name, byte[] readerToByte) {
        FileOutputStream fos = null;
        BufferedOutputStream output = null;
        try {
            File file = new File(FileNameUtils.normalize(lobs_path + File.separator + lobs_file_name, true));
            fos = new FileOutputStream(file);
            output = new BufferedOutputStream(fos);
            output.write(readerToByte, 0, readerToByte.length);
        } catch (Exception e) {
            throw new AppSystemException("大字段输出文件流时抛异常，filePath={}");
        } finally {
            try {
                if (output != null)
                    output.close();
                if (fos != null)
                    fos.close();
            } catch (IOException e0) {
                log.error("关闭流异常", e0);
            }
        }
    }

    protected DataFileWriter<Object> getAvroWriter(int[] typeArray, String hbase_name, String midName, long pageNum) throws IOException {
        DataFileWriter<Object> avroWriter = null;
        for (int type : typeArray) {
            if (type == Types.BLOB || type == Types.VARBINARY || type == Types.LONGVARBINARY) {
                createLobsDir(midName);
                OutputStream outputStream = Files.newOutputStream(Paths.get(midName + "LOB" + File.separator + "avro_" + hbase_name + pageNum));
                avroWriter = new DataFileWriter<>(new GenericDatumWriter<>()).setSyncInterval(100);
                avroWriter.setCodec(CodecFactory.snappyCodec());
                avroWriter.create(SCHEMA, outputStream);
                break;
            }
        }
        return avroWriter;
    }

    private void createLobsDir(String midName) {
        File file = new File(midName + "LOB");
        if (!file.exists()) {
            boolean mkdirs = file.mkdirs();
            log.info("创建文件夹" + midName + "LOB" + mkdirs);
        }
        File file2 = new File(midName + "LOBS");
        if (!file2.exists()) {
            boolean mkdirs = file2.mkdirs();
            log.info("创建文件夹" + midName + "LOBS" + mkdirs);
        }
    }

    protected DataFileWriter<Object> getAvroWriter(Map<String, Integer> typeMap, String hbase_name, String midName, long pageNum) throws IOException {
        DataFileWriter<Object> avroWriter = null;
        for (String key : typeMap.keySet()) {
            Integer type = typeMap.get(key);
            if (type == Types.BLOB || type == Types.VARBINARY || type == Types.LONGVARBINARY) {
                createLobsDir(midName);
                OutputStream outputStream = Files.newOutputStream(Paths.get(midName + "LOB" + File.separator + "avro_" + hbase_name + pageNum));
                avroWriter = new DataFileWriter<>(new GenericDatumWriter<>()).setSyncInterval(100);
                avroWriter.setCodec(CodecFactory.snappyCodec());
                avroWriter.create(SCHEMA, outputStream);
                break;
            }
        }
        return avroWriter;
    }

    protected String toMD5(String plainText) {
        return MD5Util.md5String(plainText);
    }

    public static String columnToFixed(String columnValue, int length, String database_code, String column_name) {
        StringBuilder sb;
        try {
            byte[] bytes = columnValue.getBytes(DataBaseCode.ofValueByCode(database_code));
            int columnValueLength = bytes.length;
            sb = new StringBuilder();
            sb.append(columnValue);
            if (length >= columnValueLength) {
                for (int j = 0; j < length - columnValueLength; j++) {
                    sb.append(' ');
                }
            } else {
                throw new AppSystemException(column_name + "字段定长指定的长度小于源数据长度；字段自定长度为：" + length + ",实际值长度为：" + columnValueLength);
            }
            return sb.toString();
        } catch (UnsupportedEncodingException e) {
            throw new AppSystemException("定长文件卸数编码读取错误", e);
        }
    }

    protected List<Integer> stringToIntegerList(List<String> list) {
        List<Integer> integerList = new ArrayList<>();
        for (String string : list) {
            integerList.add(Integer.parseInt(string));
        }
        return integerList;
    }

    protected Map<String, Boolean> transMd5ColMap(Map<String, Boolean> md5ColMap) {
        Map<String, Boolean> map = new HashMap<>();
        boolean flag = true;
        for (String key : md5ColMap.keySet()) {
            if (md5ColMap.get(key)) {
                flag = false;
                break;
            }
        }
        if (flag) {
            for (String key : md5ColMap.keySet()) {
                map.put(key, true);
            }
        } else {
            map = md5ColMap;
        }
        return map;
    }

    protected void appendOperateInfo(StringBuilder sb, String database_separatorr) {
        if (JobConstant.ISADDOPERATEINFO) {
            sb.append(database_separatorr).append(operateDate).append(database_separatorr).append(operateTime).append(database_separatorr).append(user_id);
        }
    }
}
