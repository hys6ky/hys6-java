package hyren.serv6.commons.utils.constant;

import hyren.serv6.base.codes.IsFlag;
import org.mapdb.Atomic;
import java.io.File;

public class CommonVariables {

    public static final String PREFIX = PropertyParaValue.getString("pathprefix", "/hrds");

    public static final String ISLRELEASE = PREFIX + File.separator + PathUtil.ISL + File.separator;

    public static final String DCLRELEASE = PREFIX + File.separator + PathUtil.DCL + File.separator;

    public static final String DPLRELEASE = PREFIX + File.separator + PathUtil.DPL + File.separator;

    public static final String DMLRELEASE = PREFIX + File.separator + PathUtil.DML + File.separator;

    public static final String SFLRELEASE = PREFIX + File.separator + PathUtil.SFL + File.separator;

    public static final String AMLRELEASE = PREFIX + File.separator + PathUtil.AML + File.separator;

    public static final String DQCRELEASE = PREFIX + File.separator + PathUtil.DQC + File.separator;

    public static final String UDLRELEASE = PREFIX + File.separator + PathUtil.UDL + File.separator;

    public static final String TMPDIR = PREFIX + File.separator + "TMP";

    public static final boolean FILE_COLLECTION_IS_WRITE_HADOOP = IsFlag.Shi.getCode().equals(PropertyParaValue.getString("file_collection_is_write_hadoop", IsFlag.Fou.getCode()));

    public static final String PRINCIPLE_NAME = PropertyParaValue.getString("principle.name", "hyshf@beyondsoft.com");

    public static final String SOLR_IMPL_CLASS_NAME = PropertyParaValue.getString("solrclassname", "hyren.serv6.hadoop.commons.solr.impl.SolrOperatorImpl_7");

    public static final String SOLR_COLLECTION = PropertyParaValue.getString("collection", "HrdsFullTextIndexing");

    public static final int SOLR_BULK_SUBMISSIONS_NUM = PropertyParaValue.getInt("solr_bulk_submissions_num", 50000);

    public static final String ZK_HOST = PropertyParaValue.getString("zkHost", "hdp001:2181,hdp002:2181,hdp003:2181/solr");

    public static final String AUTHORITY = PropertyParaValue.getString("restAuthority", "");

    public static final String RESTFILEPATH = PropertyParaValue.getString("restFilePath", "");

    public static final String ALGORITHMS_RESULT_ROOT_PATH = PropertyParaValue.getString("algorithms_result_root_path", "");

    public static final int SFTP_PORT = PropertyParaValue.getInt("sftp_port", 22);

    public static final String OCR_RPC_ADDRESS = PropertyParaValue.getString("ocr_rpc_address", "127.0.0.1:18101");

    public static final boolean USE_OCR_RPC = IsFlag.Shi.getCode().equals(PropertyParaValue.getString("use_ocr_rpc", "0"));

    public static final int OCR_THREAD_POOL = PropertyParaValue.getInt("ocr_thread_pool", 4);

    public static final String OCR_RECOGNITION_LANGUAGE = PropertyParaValue.getString("ocr_recognition_language", "chi_sim");

    public static final int SUMMARY_VOLUMN = PropertyParaValue.getInt("summary_volumn", 3);

    public static final int FULL_TEXT_SEARCH_BATCH_NUM = PropertyParaValue.getInt("full_text_search_batch_num", 500);

    public static final String SPARK_HOME = PropertyParaValue.getString("spark_home", "/data/project/hyren/hrsapp/dist_6/java/spark");

    public static final int DB_BATCH_ROW = PropertyParaValue.getInt("dbBatch_row", 5000);

    public static final File COLLECT_AGENT_JAR_PATH = new File(PropertyParaValue.getString("agentpath", "hyren-serv6-agent-6.0.jar"));

    public static final File STREAM_AGENT_JAR_PATH = new File(PropertyParaValue.getString("kafkaAgentPath", "hyren-serv6-stream-agent-6.0.jar"));

    public static final Long AGENT_LISTENER_PERIOD = PropertyParaValue.getLong("agent.listener.period", 10L);
}
