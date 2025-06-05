package hyren.serv6.commons.utils.agent.constant;

import hyren.serv6.base.codes.IsFlag;
import java.io.File;

public class JobConstant {

    public static final int BUFFER_ROW = Integer.parseInt(PropertyParaUtil.getString("dbBatch_row", "5000"));

    public static final boolean FILE_COLLECTION_IS_WRITE_HADOOP = IsFlag.Shi.getCode().equals(PropertyParaUtil.getString("file_collection_is_write_hadoop", IsFlag.Fou.getCode()));

    public static final long FILE_BLOCKSIZE = Long.parseLong(PropertyParaUtil.getString("file_blocksize", "1024")) * 1024 * 1024L;

    public static final boolean WriteMultipleFiles = IsFlag.Shi.getCode().equals(PropertyParaUtil.getString("writemultiplefiles", IsFlag.Fou.getCode()));

    public static final boolean FILECHANGESTYPEMD5 = "MD5".equals(PropertyParaUtil.getString("determineFileChangesType", ""));

    public static final int SUMMARY_VOLUMN = Integer.parseInt(PropertyParaUtil.getString("summary_volumn", "3"));

    public static final long SINGLE_AVRO_SIZE = Long.parseLong(PropertyParaUtil.getString("singleAvroSize", "134217728"));

    public static final long THRESHOLD_FILE_SIZE = Long.parseLong(PropertyParaUtil.getString("thresholdFileSize", "26214400"));

    public static final String PREFIX = PropertyParaUtil.getString("pathprefix", "/hrds");

    public static final boolean ISADDOPERATEINFO = Boolean.parseBoolean(PropertyParaUtil.getString("isAddOperateInfo", "false"));

    public static final boolean ISWRITEDICTIONARY = Boolean.parseBoolean(PropertyParaUtil.getString("isWriteDictionary", "false"));

    public static final int AVAILABLEPROCESSORS = Integer.parseInt(PropertyParaUtil.getString("availableProcessors", String.valueOf(Runtime.getRuntime().availableProcessors())));

    public static final String SOLRCLASSNAME = PropertyParaUtil.getString("solrclassname", "");

    public static final String SOLRZKHOST = PropertyParaUtil.getString("zkHost", "");

    public static final String SOLRCOLLECTION = PropertyParaUtil.getString("collection", "");

    public static final String TMPDIR = PREFIX + File.separator + "TMP";

    public static final String agentConfigPath = PropertyParaUtil.getString("agentConfigPath", "");

    public static final String DICTIONARY = agentConfigPath + File.separator + "dictionary" + File.separator;

    public static final String MESSAGEFILE = agentConfigPath + File.separator + "messageFile" + File.separator;

    public static final String PRINCIPLE_NAME = PropertyParaUtil.getString("principle.name", "hyshf@beyondsoft.com");
}
