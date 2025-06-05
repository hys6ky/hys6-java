package hyren.serv6.agent.job.biz.core.jdbcdirectstage.service;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.MD5Util;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.fileparser.FileParserAbstract;
import hyren.serv6.commons.solr.ISolrOperator;
import hyren.serv6.commons.solr.factory.SolrFactory;
import hyren.serv6.commons.solr.param.SolrParam;
import hyren.serv6.commons.utils.agent.TableNameUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import java.io.File;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Slf4j
@DocClass(desc = "", author = "HLL", createdate = "2024/03/19 10:43")
public class ParseResultSetToSolr {

    private final ResultSet resultSet;

    private final CollectTableBean collectTableBean;

    private final TableBean tableBean;

    private final DataStoreConfBean dataStoreConfBean;

    protected String operateDate;

    protected String operateTime;

    protected String user_id;

    private final boolean is_zipper_flag;

    public ParseResultSetToSolr(ResultSet resultSet, TableBean tableBean, CollectTableBean collectTableBean, DataStoreConfBean dataStoreConfBean) {
        this.resultSet = resultSet;
        this.collectTableBean = collectTableBean;
        this.dataStoreConfBean = dataStoreConfBean;
        this.tableBean = tableBean;
        this.operateDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        this.operateTime = new SimpleDateFormat("HH:mm:ss").format(new Date());
        this.user_id = String.valueOf(collectTableBean.getUser_id());
        this.is_zipper_flag = IsFlag.Shi.getCode().equals(collectTableBean.getIs_zipper());
    }

    public long parseResultSet() {
        List<String> columnMetaInfoList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        try {
            String tableName = TableNameUtil.getUnderline1TableName(collectTableBean.getStorage_table_name(), collectTableBean.getStorage_type(), collectTableBean.getStorage_time());
            log.info("入库的目标存储层配置为: " + dataStoreConfBean.getData_store_connect_attr().toString());
            String configPath = FileNameUtils.normalize(Constant.STORECONFIGPATH + dataStoreConfBean.getDsl_name() + File.separator, true);
            Map<String, String> data_store_connect_attr = dataStoreConfBean.getData_store_connect_attr();
            SolrParam solrParam = new SolrParam();
            solrParam.setSolrZkUrl(data_store_connect_attr.get(StorageTypeKey.solr_zk_url));
            solrParam.setCollection(data_store_connect_attr.get(StorageTypeKey.collection));
            try (ISolrOperator os = SolrFactory.getSolrOperatorInstance(JobConstant.SOLRCLASSNAME, solrParam, configPath);
                SolrClient solrClient = os.getSolrClient()) {
                return addDataToSolr(tableName, solrClient);
            } catch (Exception e) {
                throw new AppSystemException("数据直连采集数据入Solr发生异常", e);
            }
        } catch (Exception e) {
            log.error("batch入库失败", e);
            throw new AppSystemException("数据库直连采集batch入库失败", e);
        }
    }

    private long addDataToSolr(String tableName, SolrClient solrClient) throws Exception {
        Map<String, Boolean> isZipperFieldInfo = FileParserAbstract.transMd5ColMap(tableBean.getIsZipperFieldInfo());
        StringBuilder md5Value = new StringBuilder();
        String etlDate = collectTableBean.getEtlDate();
        long count = 0;
        List<String> selectColumnList = StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT);
        log.info("要获取值的列信息: {}", selectColumnList);
        int[] typeArray = tableBean.getTypeArray();
        int numberOfColumns = selectColumnList.size();
        List<List<Object>> pool = new ArrayList<>();
        List<Object> listData;
        Object obj;
        while (resultSet.next()) {
            count++;
            listData = new ArrayList<>();
            log.debug("第 {} 行,开始数据处理时间: {}", count, DateUtil.getDateTime());
            for (int i = 0; i < numberOfColumns; i++) {
                listData.add(i, resultSet.getObject(selectColumnList.get(i)));
                if (is_zipper_flag && isZipperFieldInfo.get(selectColumnList.get(i)) || IsFlag.Shi == IsFlag.ofEnumByCode(collectTableBean.getIs_md5())) {
                    obj = resultSet.getObject(selectColumnList.get(i));
                    if (obj == null) {
                        obj = "";
                    }
                    md5Value.append(obj);
                }
            }
            listData.add(numberOfColumns, etlDate);
            if (is_zipper_flag) {
                listData.add(numberOfColumns + 1, Constant._MAX_DATE_8);
                listData.add(numberOfColumns + 2, MD5Util.md5String(md5Value.toString()));
                md5Value.delete(0, md5Value.length());
            } else if (IsFlag.ofEnumByCode(collectTableBean.getIs_md5()) == IsFlag.Shi) {
                StorageType storageType = StorageType.ofEnumByCode(collectTableBean.getStorage_type());
                if (storageType == StorageType.TiHuan || storageType == StorageType.ZhuiJia) {
                    listData.add(numberOfColumns + 1, MD5Util.md5String(md5Value.toString()));
                    md5Value.delete(0, md5Value.length());
                }
            }
            appendOperateInfoData(listData, numberOfColumns);
            log.debug("第 {} 行,结束数据处理时间: {}", count, DateUtil.getDateTime());
            pool.add(listData);
            if (count % JobConstant.BUFFER_ROW == 0) {
                log.info("表名: " + tableName + " , Batch {} 开始执行时间: {}", JobConstant.BUFFER_ROW, DateUtil.getDateTime());
                executeBatch(pool, solrClient, count);
                pool.clear();
                log.info("表名: " + tableName + " ,Batch 结束执行时间: {}", DateUtil.getDateTime());
            }
        }
        if (!pool.isEmpty()) {
            executeBatch(pool, solrClient, count);
            pool.clear();
        }
        return count;
    }

    private void appendOperateInfoData(List<Object> list, int numberOfColumns) {
        StorageType storageType = StorageType.ofEnumByCode(collectTableBean.getStorage_type());
        if (JobConstant.ISADDOPERATEINFO) {
            if (is_zipper_flag) {
                list.add(numberOfColumns + 3, operateDate);
                list.add(numberOfColumns + 4, operateTime);
                list.add(numberOfColumns + 5, user_id);
            } else if ((storageType == StorageType.TiHuan || storageType == StorageType.ZhuiJia) && IsFlag.ofEnumByCode(collectTableBean.getIs_md5()) == IsFlag.Shi) {
                list.add(numberOfColumns + 2, operateDate);
                list.add(numberOfColumns + 3, operateTime);
                list.add(numberOfColumns + 4, user_id);
            } else {
                list.add(numberOfColumns + 1, operateDate);
                list.add(numberOfColumns + 2, operateTime);
                list.add(numberOfColumns + 3, user_id);
            }
        }
    }

    private void executeBatch(List<List<Object>> params, SolrClient solrClient, long count) {
        if (params == null || params.isEmpty()) {
            throw new AppSystemException("处理数据进Solr时,需要处理的数据不能为空！");
        }
        try {
            params.forEach(param -> {
                List<SolrInputDocument> docs = new ArrayList<>();
                SolrInputDocument doc = new SolrInputDocument();
                StringBuilder sb = new StringBuilder();
                String storageTableName = collectTableBean.getStorage_table_name();
                List<String> columnMetaInfoList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
                boolean isMd5 = !columnMetaInfoList.contains(Constant._HYREN_MD5_VAL);
                doc.addField("table-name", storageTableName);
                for (int j = 0; j < columnMetaInfoList.size(); j++) {
                    String column_name = columnMetaInfoList.get(j).toLowerCase();
                    if (isMd5) {
                        Object value = getValue(param.get(j));
                        sb.append(value);
                        doc.addField("tf-" + column_name, value);
                    } else {
                        if (Constant._HYREN_MD5_VAL.equalsIgnoreCase(column_name)) {
                            doc.addField("id", storageTableName + "_" + getValue(param.get(j)));
                        } else {
                            doc.addField("tf-" + column_name, getValue(param.get(j)).toString());
                        }
                    }
                }
                if (isMd5) {
                    doc.addField("id", storageTableName + "_" + MD5Util.md5String(sb.toString()));
                }
                doc.addField("tf-file_text", sb.toString());
                docs.add(doc);
                doBatch(solrClient, docs, count);
            });
        } catch (Exception e) {
            throw new AppSystemException("处理数据进Solr时发生异常!", e);
        }
    }

    private void doBatch(SolrClient server, List<SolrInputDocument> docs, long count) {
        try {
            server.add(docs);
            server.commit();
            log.info("本次batch插入" + docs.size());
            log.info("数据库已插入" + count + "条！");
            docs.clear();
        } catch (Exception e) {
            throw new AppSystemException("提交数据到solr异常", e);
        }
    }

    public static Object getValue(Object tmpValue) {
        String strValue = String.valueOf(tmpValue);
        return StringUtil.isBlank(strValue) ? "" : strValue.trim();
    }

    private static ResultSet getResultSet(String collectSQL, DatabaseWrapper db) {
        ResultSet columnSet;
        try {
            String exeSql = String.format("SELECT * FROM ( %s ) HYREN_WHERE_ALIAS WHERE 1 = 2", collectSQL);
            columnSet = db.queryGetResultSet(exeSql);
        } catch (Exception e) {
            throw new AppSystemException("获取ResultSet异常", e);
        }
        return columnSet;
    }

    public static int[] getTarTypeArray(DatabaseWrapper db, String collectSQL) throws SQLException {
        ResultSet resultSet_store = getResultSet(collectSQL, db);
        ResultSetMetaData rsMetaData = resultSet_store.getMetaData();
        int num = rsMetaData.getColumnCount();
        int[] tarTypeArray = new int[num];
        for (int i = 1; i <= num; i++) {
            tarTypeArray[i - 1] = rsMetaData.getColumnType(i);
        }
        return tarTypeArray;
    }
}
