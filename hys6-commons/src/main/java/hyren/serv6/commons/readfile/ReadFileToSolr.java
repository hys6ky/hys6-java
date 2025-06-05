package hyren.serv6.commons.readfile;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.MD5Util;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.solr.ISolrOperator;
import hyren.serv6.commons.solr.factory.SolrFactory;
import hyren.serv6.commons.solr.param.SolrParam;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.DataTypeConstant;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@DocClass(desc = "", author = "zxz", createdate = "2019/12/17 15:43")
@Slf4j
public class ReadFileToSolr implements Callable<Long> {

    private final String fileAbsolutePath;

    private final CollectTableBean collectTableBean;

    private final TableBean tableBean;

    private final DataStoreConfBean dataStoreConfBean;

    private final boolean is_append;

    private final String etl_date;

    public ReadFileToSolr(String fileAbsolutePath, TableBean tableBean, CollectTableBean collectTableBean, DataStoreConfBean dataStoreConfBean) {
        this.fileAbsolutePath = fileAbsolutePath;
        this.collectTableBean = collectTableBean;
        this.dataStoreConfBean = dataStoreConfBean;
        this.tableBean = tableBean;
        this.is_append = StorageType.ZhuiJia.getCode().equals(collectTableBean.getStorage_type());
        this.etl_date = collectTableBean.getEtlDate();
    }

    @Method(desc = "", logicStep = "")
    @Override
    public Long call() {
        long count;
        String configPath = FileNameUtils.normalize(Constant.STORECONFIGPATH + dataStoreConfBean.getDsl_name() + File.separator, true);
        Map<String, String> data_store_connect_attr = dataStoreConfBean.getData_store_connect_attr();
        SolrParam solrParam = new SolrParam();
        solrParam.setSolrZkUrl(data_store_connect_attr.get(StorageTypeKey.solr_zk_url));
        solrParam.setCollection(data_store_connect_attr.get(StorageTypeKey.collection));
        try (ISolrOperator os = SolrFactory.getSolrOperatorInstance(JobConstant.SOLRCLASSNAME, solrParam, configPath);
            SolrClient solrClient = os.getSolrClient()) {
            List<String> columnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
            List<String> sourceTypeList = StringUtil.split(tableBean.getColTypeMetaInfo(), Constant.METAINFOSPLIT);
            String file_code = tableBean.getFile_code();
            FileFormat fileFormat = FileFormat.ofEnumByCode(tableBean.getFile_format());
            String column_separator = tableBean.getColumn_separator();
            boolean isMd5 = !columnList.contains(Constant._HYREN_MD5_VAL);
            String is_header = tableBean.getIs_header();
            if (fileFormat == FileFormat.CSV) {
                count = readCsvToSolr(solrClient, columnList, sourceTypeList, file_code, is_header, isMd5);
            } else if (fileFormat == FileFormat.PARQUET || fileFormat == FileFormat.ORC || fileFormat == FileFormat.SEQUENCEFILE) {
                count = ClassBase.solrWithHadoopInstance().readFileToSolr(collectTableBean, tableBean, fileAbsolutePath, solrClient, columnList, sourceTypeList);
            } else if (fileFormat == FileFormat.DingChang) {
                if (StringUtil.isEmpty(column_separator)) {
                    count = readDingChangToSolr(solrClient, columnList, sourceTypeList, file_code, is_header, isMd5);
                } else {
                    count = readFeiDingChangToSolr(solrClient, columnList, sourceTypeList, column_separator, file_code, is_header, isMd5);
                }
            } else if (fileFormat == FileFormat.FeiDingChang) {
                count = readFeiDingChangToSolr(solrClient, columnList, sourceTypeList, column_separator, file_code, is_header, isMd5);
            } else {
                throw new AppSystemException("不支持的卸数文件格式");
            }
        } catch (Exception e) {
            count = -1L;
            log.error("数据库采集读文件上传到数据库异常", e);
        }
        return count;
    }

    private long readFeiDingChangToSolr(SolrClient server, List<String> columnList, List<String> typeList, String dataDelimiter, String database_code, String is_header, boolean isMd5) {
        long num = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(fileAbsolutePath)), DataBaseCode.ofValueByCode(database_code)))) {
            String line;
            if (IsFlag.Shi.getCode().equals(is_header)) {
                line = reader.readLine();
                if (line != null) {
                    log.info("读取到表头为：" + line);
                }
            }
            List<SolrInputDocument> docs = new ArrayList<>();
            while ((line = reader.readLine()) != null) {
                num++;
                List<String> valueList = StringUtil.split(line, dataDelimiter);
                batchToSolr(server, docs, columnList, typeList, valueList, isMd5, num);
            }
            if (!docs.isEmpty()) {
                doBatch(server, docs, num);
            }
        } catch (Exception e) {
            throw new AppSystemException("bash插入数据库失败", e);
        }
        return num;
    }

    private long readDingChangToSolr(SolrClient server, List<String> columnList, List<String> typeList, String database_code, String is_header, boolean isMd5) {
        database_code = DataBaseCode.ofValueByCode(database_code);
        long num = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(fileAbsolutePath)), database_code))) {
            String line;
            List<String> lengthStrList = StringUtil.split(tableBean.getColLengthInfo(), Constant.METAINFOSPLIT);
            List<Integer> lengthList = new ArrayList<>();
            for (String lengthStr : lengthStrList) {
                lengthList.add(Integer.parseInt(lengthStr));
            }
            if (IsFlag.Shi.getCode().equals(is_header)) {
                line = reader.readLine();
                if (line != null) {
                    log.info("读取到表头为：" + line);
                }
            }
            List<SolrInputDocument> docs = new ArrayList<>();
            while ((line = reader.readLine()) != null) {
                num++;
                List<String> valueList = getDingChangValueList(line, lengthList, database_code);
                batchToSolr(server, docs, columnList, typeList, valueList, isMd5, num);
            }
            if (!docs.isEmpty()) {
                doBatch(server, docs, num);
            }
        } catch (Exception e) {
            throw new AppSystemException("bash插入数据库失败", e);
        }
        return num;
    }

    private static List<String> getDingChangValueList(String line, List<Integer> lengthList, String database_code) throws Exception {
        List<String> valueList = new ArrayList<>();
        byte[] bytes = line.getBytes(database_code);
        int begin = 0;
        for (int length : lengthList) {
            byte[] byteTmp = new byte[length];
            System.arraycopy(bytes, begin, byteTmp, 0, length);
            begin += length;
            valueList.add(new String(byteTmp, database_code));
        }
        return valueList;
    }

    public void batchToSolr(SolrClient server, List<SolrInputDocument> docs, List<String> columnList, List<String> typeList, List<String> valueList, boolean isMd5, long num) {
        SolrInputDocument doc = new SolrInputDocument();
        StringBuilder sb = new StringBuilder();
        String storageTableName = collectTableBean.getStorage_table_name();
        doc.addField("table-name", storageTableName);
        for (int j = 0; j < columnList.size(); j++) {
            if (isMd5) {
                Object value = getOrDefaultValue(typeList.get(j), valueList.get(j));
                sb.append(value);
                doc.addField("tf-" + columnList.get(j), value);
            } else {
                if (Constant._HYREN_MD5_VAL.equalsIgnoreCase(columnList.get(j))) {
                    if (is_append) {
                        doc.addField("id", storageTableName + "_" + getOrDefaultValue(typeList.get(j), valueList.get(j)) + "_" + etl_date);
                    } else {
                        doc.addField("id", storageTableName + "_" + getOrDefaultValue(typeList.get(j), valueList.get(j)));
                    }
                } else {
                    doc.addField("tf-" + columnList.get(j), getOrDefaultValue(typeList.get(j), valueList.get(j)));
                }
            }
        }
        if (isMd5) {
            if (is_append) {
                doc.addField("id", storageTableName + "_" + MD5Util.md5String(sb.toString()) + "_" + etl_date);
            } else {
                doc.addField("id", storageTableName + "_" + MD5Util.md5String(sb.toString()));
            }
        }
        docs.add(doc);
        if (num % JobConstant.BUFFER_ROW == 0) {
            doBatch(server, docs, num);
        }
    }

    public static Object getOrDefaultValue(String type, String tmpValue) {
        Object str;
        type = type.toLowerCase();
        if (type.contains(DataTypeConstant.BOOLEAN.getMessage())) {
            str = StringUtil.isBlank(tmpValue) ? null : Boolean.parseBoolean(tmpValue.trim());
        } else {
            str = StringUtil.isBlank(tmpValue) ? "" : tmpValue.trim();
        }
        return str;
    }

    private long readCsvToSolr(SolrClient server, List<String> columnList, List<String> typeList, String database_code, String is_header, boolean isMd5) {
        long num = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(fileAbsolutePath)), DataBaseCode.ofValueByCode(database_code)));
            CsvListReader csvReader = new CsvListReader(reader, CsvPreference.EXCEL_PREFERENCE)) {
            List<String> lineList;
            if (IsFlag.Shi.getCode().equals(is_header)) {
                lineList = csvReader.read();
                if (lineList != null) {
                    log.info("读取到表头为：" + lineList.toString());
                }
            }
            List<SolrInputDocument> docs = new ArrayList<>();
            while ((lineList = csvReader.read()) != null) {
                num++;
                batchToSolr(server, docs, columnList, typeList, lineList, isMd5, num);
            }
            if (!docs.isEmpty()) {
                doBatch(server, docs, num);
            }
        } catch (Exception e) {
            throw new AppSystemException("bash插入数据库失败", e);
        }
        return num;
    }

    private void doBatch(SolrClient server, List<SolrInputDocument> docs, long num) {
        try {
            server.add(docs);
            server.commit();
            log.info("本次batch插入" + docs.size());
            log.info("数据库已插入" + num + "条！");
            docs.clear();
        } catch (Exception e) {
            throw new AppSystemException("提交数据到solr异常", e);
        }
    }
}
