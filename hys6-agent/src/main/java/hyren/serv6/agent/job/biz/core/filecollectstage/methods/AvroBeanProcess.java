package hyren.serv6.agent.job.biz.core.filecollectstage.methods;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.bean.ObjectCollectParamBean;
import hyren.serv6.agent.job.biz.utils.CommunicationUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.SourceFileAttribute;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.solr.ISolrOperator;
import hyren.serv6.commons.solr.factory.SolrFactory;
import hyren.serv6.commons.solr.param.SolrParam;
import hyren.serv6.commons.utils.agent.bean.AvroBean;
import hyren.serv6.commons.utils.agent.bean.FileCollectParamBean;
import hyren.serv6.base.utils.fileutil.FileTypeUtil;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class AvroBeanProcess {

    private static final String addSql = "INSERT INTO " + SourceFileAttribute.TableName + " (agent_id,collect_set_id,collect_type,file_avro_block,file_avro_path,file_id,file_md5,file_size,file_suffix," + " file_type,hbase_name,is_big_file,is_in_hbase,meta_info,original_name,original_update_date,original_update_time," + " source_id,source_path,storage_date,storage_time,table_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String updateSql = "UPDATE " + SourceFileAttribute.TableName + " SET" + " storage_date = ?,storage_time = ?,original_update_date = ?,Original_update_time = ?,file_md5 = ?,file_avro_path = ?," + " file_avro_block = ? WHERE file_id = ?";

    private final FileCollectParamBean fileCollectParamBean;

    private final String sysDate;

    private final String job_rs_id;

    public AvroBeanProcess(FileCollectParamBean fileCollectParamBean, String sysDate, String job_rs_id) {
        this.fileCollectParamBean = fileCollectParamBean;
        this.sysDate = sysDate;
        this.job_rs_id = job_rs_id;
    }

    public List<String[]> saveMetaData(List<AvroBean> avroBeans, ConcurrentMap<String, String> fileNameHTreeMap) {
        log.info("保存已采集文件信息到元数据库开始...");
        List<Object[]> addParamsPool = new ArrayList<>();
        List<Object[]> updateParamsPool = new ArrayList<>();
        List<String[]> hbaseList = new ArrayList<>();
        try {
            for (AvroBean avroBean : avroBeans) {
                SourceFileAttribute attribute = new SourceFileAttribute();
                String fileId = avroBean.getUuid();
                String rowKey;
                if (IsFlag.Fou.getCode().equals(avroBean.getIs_increasement())) {
                    Object[] addFileAttributeList = new Object[22];
                    attribute.setAgent_id(fileCollectParamBean.getAgent_id());
                    attribute.setCollect_set_id(Long.parseLong(fileCollectParamBean.getFcs_id()));
                    attribute.setCollect_type(AgentType.WenJianXiTong.getCode());
                    attribute.setFile_avro_block(Long.parseLong(avroBean.getFile_avro_block()));
                    attribute.setFile_avro_path(avroBean.getFile_avro_path());
                    attribute.setFile_id(avroBean.getUuid());
                    attribute.setFile_md5(avroBean.getFile_md5());
                    attribute.setFile_size(Long.parseLong(avroBean.getFile_size()));
                    attribute.setFile_suffix(FilenameUtils.getExtension(avroBean.getFile_name()));
                    attribute.setFile_type(FileTypeUtil.fileTypeCode(FilenameUtils.getExtension(avroBean.getFile_name())));
                    attribute.setHbase_name(ClassBase.hadoopInstance().byteToString((ObjectCollectParamBean.getFile_name())));
                    attribute.setIs_big_file(avroBean.getIs_big_file());
                    attribute.setIs_in_hbase("");
                    attribute.setMeta_info("");
                    attribute.setOriginal_name(avroBean.getFile_name());
                    attribute.setOriginal_update_date(stringToDate(avroBean.getFile_time(), "yyyyMMdd"));
                    attribute.setOriginal_update_time(stringToDate(avroBean.getFile_time(), "HHmmss"));
                    attribute.setSource_id(fileCollectParamBean.getSource_id());
                    attribute.setSource_path(avroBean.getFile_scr_path());
                    attribute.setStorage_date(DateUtil.getSysDate());
                    attribute.setStorage_time(DateUtil.getSysTime());
                    attribute.setTable_name("");
                    addFileAttributeList[0] = attribute.getAgent_id();
                    addFileAttributeList[1] = attribute.getCollect_set_id();
                    addFileAttributeList[2] = attribute.getCollect_type();
                    addFileAttributeList[3] = attribute.getFile_avro_block();
                    addFileAttributeList[4] = attribute.getFile_avro_path();
                    addFileAttributeList[5] = attribute.getFile_id();
                    addFileAttributeList[6] = attribute.getFile_md5();
                    addFileAttributeList[7] = attribute.getFile_size();
                    addFileAttributeList[8] = attribute.getFile_suffix();
                    addFileAttributeList[9] = attribute.getFile_type();
                    addFileAttributeList[10] = attribute.getHbase_name();
                    addFileAttributeList[11] = attribute.getIs_big_file();
                    addFileAttributeList[12] = attribute.getIs_in_hbase();
                    addFileAttributeList[13] = attribute.getMeta_info();
                    addFileAttributeList[14] = attribute.getOriginal_name();
                    addFileAttributeList[15] = attribute.getOriginal_update_date();
                    addFileAttributeList[16] = attribute.getOriginal_update_time();
                    addFileAttributeList[17] = attribute.getSource_id();
                    addFileAttributeList[18] = attribute.getSource_path();
                    addFileAttributeList[19] = attribute.getStorage_date();
                    addFileAttributeList[20] = attribute.getStorage_time();
                    addFileAttributeList[21] = attribute.getTable_name();
                    addParamsPool.add(addFileAttributeList);
                    rowKey = fileId + "_" + avroBean.getFile_md5();
                } else if (IsFlag.Shi.getCode().equals(avroBean.getIs_increasement())) {
                    Object[] updateFileAttributeList = new Object[8];
                    attribute.setStorage_date(DateUtil.getSysDate());
                    attribute.setStorage_time(DateUtil.getSysTime());
                    attribute.setOriginal_update_date(stringToDate(avroBean.getFile_time(), "yyyyMMdd"));
                    attribute.setOriginal_update_time(stringToDate(avroBean.getFile_time(), "HHmmss"));
                    attribute.setFile_md5(avroBean.getFile_md5());
                    attribute.setFile_size(Long.parseLong(avroBean.getFile_size()));
                    attribute.setFile_avro_path(avroBean.getFile_avro_path());
                    attribute.setFile_avro_block(Long.parseLong(avroBean.getFile_avro_block()));
                    attribute.setFile_id(fileId);
                    updateFileAttributeList[0] = attribute.getStorage_date();
                    updateFileAttributeList[1] = attribute.getStorage_time();
                    updateFileAttributeList[2] = attribute.getOriginal_update_date();
                    updateFileAttributeList[3] = attribute.getOriginal_update_time();
                    updateFileAttributeList[4] = attribute.getFile_md5();
                    updateFileAttributeList[5] = attribute.getFile_avro_path();
                    updateFileAttributeList[6] = attribute.getFile_avro_block();
                    updateFileAttributeList[7] = attribute.getFile_id();
                    updateParamsPool.add(updateFileAttributeList);
                    String md5 = JsonUtil.toObject(JsonUtil.toJson(fileNameHTreeMap.get(avroBean.getFile_scr_path())), new TypeReference<Map<String, Object>>() {
                    }).get("file_md5").toString();
                    String[] guanlian = new String[] { fileId + "_" + md5 };
                    hbaseList.add(guanlian);
                    rowKey = fileId + "_" + avroBean.getFile_md5();
                } else {
                    throw new AppSystemException("Is_increasement: 的值异常===" + avroBean.getIs_increasement());
                }
                String[] zengliang = new String[] { rowKey, avroBean.getFile_md5(), avroBean.getFile_avro_path(), avroBean.getFile_avro_block() };
                hbaseList.add(zengliang);
            }
            log.info("addParamsPool.size(): " + addParamsPool.size());
            if (!addParamsPool.isEmpty()) {
                log.info("执行全量插入数据（addParamsPool）（executeBatch...）");
                CommunicationUtil.batchAddSourceFileAttribute(addParamsPool, addSql, job_rs_id);
            }
            log.info("updateParamsPool.size(): " + updateParamsPool.size());
            if (!updateParamsPool.isEmpty()) {
                log.info("执行增量插入数据（updateParamsPool）（executeBatch...）");
                CommunicationUtil.batchUpdateSourceFileAttribute(updateParamsPool, updateSql, job_rs_id);
            }
            log.info("保存已采集文件信息到元数据库结束...");
        } catch (Exception e) {
            log.error("保存文件信息到元数据库异常", e);
            throw new AppSystemException("保存文件信息到元数据库异常" + e.getMessage());
        }
        return hbaseList;
    }

    public void saveInHbase(List<String[]> hbaseList) {
        ClassBase.HbaseInstance().saveInHbase(hbaseList, sysDate);
    }

    public void saveInSolr(List<AvroBean> avroBeans) {
        log.info("开始进solr...");
        long start = System.currentTimeMillis();
        int count = 0;
        int commitNumber = 500;
        SolrParam solrParam = new SolrParam();
        solrParam.setSolrZkUrl(JobConstant.SOLRZKHOST);
        solrParam.setCollection(JobConstant.SOLRCOLLECTION);
        try (ISolrOperator os = SolrFactory.getSolrOperatorInstance(JobConstant.SOLRCLASSNAME, solrParam, System.getProperty("user.dir") + File.separator + "conf" + File.separator)) {
            SolrClient server = os.getSolrClient();
            List<SolrInputDocument> docs = new ArrayList<>();
            SolrInputDocument doc;
            for (AvroBean avroBean : avroBeans) {
                doc = new SolrInputDocument();
                doc.addField("id", avroBean.getUuid());
                doc.addField("tf-collect_type", AgentType.WenJianXiTong.getCode());
                doc.addField("tf-file_name", avroBean.getFile_name());
                doc.addField("tf-file_scr_path", avroBean.getFile_scr_path());
                doc.addField("tf-file_size", avroBean.getFile_size());
                doc.addField("tf-file_time", avroBean.getFile_time());
                doc.addField("tf-file_summary", avroBean.getFile_summary());
                doc.addField("tf-file_text", avroBean.getFile_text());
                doc.addField("tf-file_md5", avroBean.getFile_md5());
                doc.addField("tf-file_avro_path", avroBean.getFile_avro_path());
                doc.addField("tf-file_avro_block", avroBean.getFile_avro_block());
                doc.addField("tf-is_big_file", avroBean.getIs_big_file());
                doc.addField("tf-file_suffix", FilenameUtils.getExtension(avroBean.getFile_name()));
                doc.addField("tf-storage_date", DateUtil.getSysDate());
                doc.addField("tf-fcs_id", fileCollectParamBean.getFcs_id());
                doc.addField("tf-fcs_name", fileCollectParamBean.getFcs_name());
                doc.addField("tf-agent_id", fileCollectParamBean.getAgent_id());
                doc.addField("tf-agent_name", fileCollectParamBean.getAgent_name());
                doc.addField("tf-source_id", fileCollectParamBean.getSource_id());
                doc.addField("tf-datasource_name", fileCollectParamBean.getDatasource_name());
                doc.addField("tf-dep_id", fileCollectParamBean.getDep_id());
                docs.add(doc);
                count++;
                if (count % commitNumber == 0) {
                    server.add(docs);
                    server.commit();
                    docs.clear();
                    log.info("[info] " + count + " 条数据完成索引！");
                }
            }
            if (!docs.isEmpty()) {
                server.add(docs);
                server.commit();
                log.info("一共" + count + " 条数据完成索引！");
                docs.clear();
            }
            log.info("成功建立solr索引,共耗时：" + (System.currentTimeMillis() - start) * 1.0 / 1000 + "秒");
        } catch (Exception e) {
            log.error("数据进solr失败...", e);
            throw new AppSystemException("数据进solr失败..." + e.getMessage());
        }
    }

    private String stringToDate(String lo, String format) {
        long time = Long.parseLong(lo);
        Date date = new Date(time);
        SimpleDateFormat sd = new SimpleDateFormat(format);
        return sd.format(date);
    }
}
