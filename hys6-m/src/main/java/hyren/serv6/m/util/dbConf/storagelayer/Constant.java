package hyren.serv6.m.util.dbConf.storagelayer;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.FileUtil;
import hyren.serv6.base.codes.CleanType;
import hyren.serv6.base.codes.FileFormat;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "zxz", createdate = "2019/12/25 0025 下午 04:28")
public class Constant {

    public static final String SDATENAME = "HYREN_S_DATE";

    public static final String EDATENAME = "HYREN_E_DATE";

    public static final String MD5NAME = "HYREN_MD5_VAL";

    public static final String HYREN_OPER_DATE = "HYREN_OPER_DATE";

    public static final String HYREN_OPER_TIME = "HYREN_OPER_TIME";

    public static final String HYREN_OPER_PERSON = "HYREN_OPER_PERSON";

    public static final String TABLE_ID_NAME = "HYREN_TABLE_ID";

    private static final String[] HYRENCOLUMN = { SDATENAME, EDATENAME, MD5NAME, HYREN_OPER_DATE, HYREN_OPER_TIME, HYREN_OPER_PERSON, TABLE_ID_NAME };

    public static final List<String> HYRENFIELD = Arrays.asList(HYRENCOLUMN);

    public static final String MAXDATE = "99991231";

    public static final byte[] HBASE_COLUMN_FAMILY = "F".getBytes();

    public static final String HBASE_ROW_KEY = "hyren_key";

    private static final String USER_DIR = System.getProperty("user.dir");

    public static final String JOBINFOPATH = USER_DIR + File.separator + "jobInfo" + File.separator;

    public static final String XMLPATH = USER_DIR + File.separator + "xmlPath" + File.separator;

    public static final String JOBFILENAME = "jobInfo.json";

    public static final String MAPDBPATH = USER_DIR + File.separator + "mapDb" + File.separator;

    public static final String FILEUNLOADFOLDER = USER_DIR + File.separator + "dirFile" + File.separator;

    public static final String DBFILEUNLOADFOLDER = USER_DIR + File.separator + "dbFile" + File.separator;

    public static final String STORECONFIGPATH = USER_DIR + File.separator + "storeConfigPath" + File.separator;

    public static final String HDFSSHELLFILE = USER_DIR + File.separator + "hdfsShellFile" + File.separator;

    public static final String COMMUNICATIONERRORFOLDER = USER_DIR + File.separator + "CommunicationError" + File.separator;

    public static final String PARALLEL_SQL_START = "#{hy_start}";

    public static final String PARALLEL_SQL_END = "#{hy_end}";

    public static final String DCL_BATCH = "dcl_batch";

    public static final String DCL_REALTIME = "dcl_realtime";

    public static final String SYS_DATA_TABLE = "sys_data_table";

    public static final String SYS_DATA_BAK = "sys_data_bak";

    public static final String DQC_TABLE = "dqc_";

    public static final String DQC_INVALID_TABLE = "dit_";

    public static final String DM_SET_INVALID_TABLE = "set_invalid";

    public static final String DM_RESTORE_TABLE = "restore";

    public static final String HANDLER = "/reloadDictionary";

    public static final char SOLR_DATA_DELIMITER = '\001';

    public static final String USERLOGIN = "用户登入";

    public static final String USERSIGNOUT = "用户登出";

    public static final String INVALIDUSERINFO = "用户信息失效";

    public static final String DELETEDBTASK = "采集数据库直连任务的删除";

    public static final String DELETEDFTASK = "采集db文件任务的删除记录";

    public static final String DELETEHALFSTRUCTTASK = "采集半结构任务的删除记录";

    public static final String DELETENONSTRUCTTASK = "采集非结构任务的删除记录";

    public static final String DELETEFTPTASK = "采集ftp任务的删除记录";

    public static final String ADDDMDATATABLE = "集市新建数据表";

    public static final String DELETEDMDATATABLE = "集市删除数据表";

    public static final String METAINFOSPLIT = "^";

    public static final String SQLDELIMITER = "`@^";

    public static final String DEFAULTLINESEPARATOR = "\n";

    public static final String DEFAULTLINESEPARATORSTR = "\\n";

    public static final String DATADELIMITER = "`@^";

    public static final String SEQUENCEDELIMITER = String.valueOf('\001');

    public static final String ETLPARASEPARATOR = "@";

    public static final Map<String, String> fileFormatMap = new HashMap<>();

    public static final String COLLECT_JOB_COMMAND = "collect-job-command.sh";

    public static final String UNSTRUCTURED_COLLECTION = "unstructured-job-command.sh";

    public static final String BATCH_DATE = "#{txdate}";

    public static final String XS_ZT_RESOURCE_TYPE = "XS_ZT";

    public static final String NORMAL_DEFAULT_RESOURCE_TYPE = "normalDefType";

    public static final int RESOURCE_NUM = 15;

    public static final int JOB_RESOURCE_NUM = 1;

    public static final String HYRENBIN = "!{HYSHELLBIN}";

    public static final String HYRENLOG = "!{HYLOG}";

    public static final String PARA_HYRENBIN = "!HYSHELLBIN";

    public static final String PARA_HYRENLOG = "!HYLOG";

    public static final String SPLITTER = "_";

    public static final String START_AGENT = "agent-operation.sh";

    public static final String STREAM_START_AGENT = "stream_agent_operation.sh";

    public static final String CONTROL_OPERATION_SH = "control-operation.sh";

    public static final String TRIGGER_OPERATION_SH = "trigger-operation.sh";

    public static final Map<String, Integer> DEFAULT_TABLE_CLEAN_ORDER = new HashMap<>();

    public static final Map<String, Integer> DEFAULT_COLUMN_CLEAN_ORDER = new HashMap<>();

    public static final Map<String, Integer> DATABASE_CLEAN = new HashMap<>();

    static {
        fileFormatMap.put(FileFormat.FeiDingChang.getCode(), "NONFIXEDFILE");
        fileFormatMap.put(FileFormat.DingChang.getCode(), "FIXEDFILE");
        fileFormatMap.put(FileFormat.CSV.getCode(), FileFormat.CSV.getValue());
        fileFormatMap.put(FileFormat.PARQUET.getCode(), FileFormat.PARQUET.getValue());
        fileFormatMap.put(FileFormat.ORC.getCode(), FileFormat.ORC.getValue());
        fileFormatMap.put(FileFormat.SEQUENCEFILE.getCode(), FileFormat.SEQUENCEFILE.getValue());
        DEFAULT_TABLE_CLEAN_ORDER.put(CleanType.ZiFuBuQi.getCode(), 1);
        DEFAULT_TABLE_CLEAN_ORDER.put(CleanType.ZiFuTiHuan.getCode(), 2);
        DEFAULT_TABLE_CLEAN_ORDER.put(CleanType.ZiFuHeBing.getCode(), 3);
        DEFAULT_TABLE_CLEAN_ORDER.put(CleanType.ZiFuTrim.getCode(), 4);
        DEFAULT_COLUMN_CLEAN_ORDER.put(CleanType.ZiFuBuQi.getCode(), 1);
        DEFAULT_COLUMN_CLEAN_ORDER.put(CleanType.ZiFuTiHuan.getCode(), 2);
        DEFAULT_COLUMN_CLEAN_ORDER.put(CleanType.ShiJianZhuanHuan.getCode(), 3);
        DEFAULT_COLUMN_CLEAN_ORDER.put(CleanType.MaZhiZhuanHuan.getCode(), 4);
        DEFAULT_COLUMN_CLEAN_ORDER.put(CleanType.ZiFuChaiFen.getCode(), 5);
        DEFAULT_COLUMN_CLEAN_ORDER.put(CleanType.ZiFuTrim.getCode(), 6);
        DATABASE_CLEAN.put(CleanType.ZiFuBuQi.getCode(), 1);
        DATABASE_CLEAN.put(CleanType.ZiFuTiHuan.getCode(), 2);
        DATABASE_CLEAN.put(CleanType.ShiJianZhuanHuan.getCode(), 3);
        DATABASE_CLEAN.put(CleanType.MaZhiZhuanHuan.getCode(), 4);
        DATABASE_CLEAN.put(CleanType.ZiFuHeBing.getCode(), 5);
        DATABASE_CLEAN.put(CleanType.ZiFuChaiFen.getCode(), 6);
        DATABASE_CLEAN.put(CleanType.ZiFuTrim.getCode(), 7);
    }

    public static final String PROJECT_BIN_DIR = System.getProperty("user.dir") + File.separator + "bin";

    public static final String DEFAULT_TABLE_SPACE = "hyshf";

    public static final String SPACE = " ";

    public static final String LXKH = "(";

    public static final String RXKH = ")";

    public static final String COLLECTOKFILE = "hyren_collect_flag.ok";

    public static final String MARKETDELIMITER = "-->";

    public static final String HIVEMAPPINGROWKEY = "rowkey_hyren";

    public static final String CUSTOMIZE = "customize";

    public static final String DASHBOARDINTERFACENAME = "dashboardRelease";

    public static final String ALGORITHMS_CONF_SERIALIZE_PATH = FileUtil.TEMP_DIR_NAME + "algorithms-conf-serialize" + FileUtil.PATH_SEPARATOR_CHAR;

    public static final String HYUCC_RESULT_PATH_NAME = "HyUccOut";

    public static final String HYFD_RESULT_PATH_NAME = "HyFdOut";

    public static final String STREAM_HYREN_END = "hyren_end_end_end";

    public static final String SOLR_DATA_ASSOCIATION_PREFIX = "tf-h_";
}
