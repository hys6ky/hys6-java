package hyren.serv6.commons.utils;

import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.commons.config.httpconfig.HttpServerConf;
import hyren.serv6.base.entity.AgentDownInfo;
import hyren.serv6.base.entity.AgentInfo;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import java.util.ArrayList;
import java.util.List;

public class AgentActionUtil {

    private static final List<String> list;

    public static final String GETSERVERINFO = "/agentserver/getServerInfo";

    public static final String GETSYSTEMFILEINFO = "/agentserver/getSystemFileInfo";

    public static final String TESTCONNECTION = "/testConn/testConn";

    public static final String BATCHADDSOURCEFILEATTRIBUTE = "/receive/batchAddSourceFileAttribute";

    public static final String BATCHUPDATESOURCEFILEATTRIBUTE = "/receive/batchUpdateSourceFileAttribute";

    public static final String GETDATABASETABLE = "/database/getDatabaseTable";

    public static final String GETTABLECOLUMN = "/database/getTableColumn";

    public static final String GETAlLLTABLECOLUMN = "/database/getAllTableColumn";

    public static final String GETALLTABLESTORAGE = "/database/getAllTableStorage";

    public static final String SAVECOLLECTCASE = "/receive/saveCollectCase";

    public static final String EXECUTEFILECOLLECT = "/unstructuredfilecollect/execute";

    public static final String EXECUTEFILECOLLECTIMMEDIATELY = "/unstructuredfilecollect/executeImmediately";

    public static final String TESTPARALLELSQL = "/testConn/testParallelSQL";

    public static final String GETTABLECOUNT = "/testConn/getTableCount";

    public static final String GETCUSTCOLUMN = "/database/getCustColumn";

    public static final String ADDSOURCEFILEATTRIBUTE = "/receive/addSourceFileAttribute";

    public static final String ADDDATASTOREREG = "/receive/addDataStoreReg";

    public static final String SENDJDBCCOLLECTTASKINFO = "/jdbccollect/execute";

    public static final String SENDDBCOLLECTTASKINFO = "/dbfilecollect/execute";

    public static final String SENDJDBCDIRECTTASKINFO = "/jdbcdirectcollect/execute";

    public static final String JDBCCOLLECTEXECUTEIMMEDIATELY = "/jdbccollect/executeImmediately";

    public static final String DBCOLLECTEXECUTEIMMEDIATELY = "/dbfilecollect/executeImmediately";

    public static final String JDBCDIRECTEXECUTEIMMEDIATELY = "/jdbcdirectcollect/executeImmediately";

    public static final String GETDICTIONARYJSON = "/jdbccollect/getDictionaryJson";

    public static final String BATCHADDFTPTRANSFER = "/receive/batchAddFtpTransfer";

    public static final String SENDFTPCOLLECTTASKINFO = "/ftpcollect/execute";

    public static final String GETDICTABLE = "/semistructured/getDicTable";

    public static final String GETALLDICCOLUMNS = "/semistructured/getAllDicColumns";

    public static final String GETALLHANDLETYPE = "/semistructured/getAllHandleType";

    public static final String GETFIRSTLINEDATA = "/semistructured/getFirstLineData";

    public static final String WRITEDICTIONARY = "/semistructured/writeDictionary";

    public static final String OBJECTCOLLECTEXECUTE = "/semistructured/execute";

    public static final String OBJECTCOLLECTEXECUTEIMMEDIATELY = "/semistructured/executeImmediately";

    public static final String COMPUTERRESOURCE = "/resourceused/readResourceInfo";

    public static final String SINGLEJOB = "/single/executeSingleJob";

    public static final String UNLOADDISTRIBUTEDATA = "/receive/distribute/unloadDistributeData";

    public static final String UNLOADRECEIVEDATA = "/dataReception/unloadAnalData";

    public static final String CDC_EXECUTE = "/cdccollect/execute";

    public static final String CDC_ABORT = "/cdccollect/abort";

    public static final String CDC_STATUS = "/cdccollect/status";

    public static final String START_CDC_SYNC_PROGRAM = "/cdccollect/startSync";

    public static final String START_CDC_COLLECT_PROGRAM = "/cdccollect/startCollect";

    public static final String CDC_COLLECT_GETPARAM = "/receive/cdc/getCollectParam";

    public static final String CDC_SYNC_GETPARAM = "/receive/cdc/getSyncParam";

    public static final String CDC_COLLECT_RUNSTATE = "/receive/cdc/updateFlinkInfo/collectRun";

    public static final String CDC_SYNC_RUNSTATE = "/receive/cdc/updateFlinkInfo/syncRun";

    public static final String CDC_COLLECT_FAILEDSTATE = "/receive/cdc/updateFlinkInfo/collectFailed";

    public static final String CDC_SYNC_FAILEDSTATE = "/receive/cdc/updateFlinkInfo/syncFailed";

    public static final String CDC_ADDDATASTOREREG = "/receive/cdc/addDataStoreReg";

    static {
        list = new ArrayList<>();
        list.add(GETSERVERINFO);
        list.add(GETSYSTEMFILEINFO);
        list.add(TESTCONNECTION);
        list.add(BATCHADDSOURCEFILEATTRIBUTE);
        list.add(BATCHUPDATESOURCEFILEATTRIBUTE);
        list.add(GETDATABASETABLE);
        list.add(GETTABLECOLUMN);
        list.add(SAVECOLLECTCASE);
        list.add(EXECUTEFILECOLLECT);
        list.add(TESTPARALLELSQL);
        list.add(GETTABLECOUNT);
        list.add(GETCUSTCOLUMN);
        list.add(ADDSOURCEFILEATTRIBUTE);
        list.add(SENDJDBCCOLLECTTASKINFO);
        list.add(SENDDBCOLLECTTASKINFO);
        list.add(BATCHADDFTPTRANSFER);
        list.add(SENDFTPCOLLECTTASKINFO);
        list.add(GETDICTABLE);
        list.add(GETALLDICCOLUMNS);
        list.add(GETALLHANDLETYPE);
        list.add(WRITEDICTIONARY);
        list.add(ADDDATASTOREREG);
        list.add(GETAlLLTABLECOLUMN);
        list.add(GETALLTABLESTORAGE);
        list.add(GETDICTIONARYJSON);
        list.add(GETFIRSTLINEDATA);
        list.add(DBCOLLECTEXECUTEIMMEDIATELY);
        list.add(JDBCCOLLECTEXECUTEIMMEDIATELY);
        list.add(JDBCDIRECTEXECUTEIMMEDIATELY);
        list.add(SENDJDBCDIRECTTASKINFO);
        list.add(EXECUTEFILECOLLECTIMMEDIATELY);
        list.add(OBJECTCOLLECTEXECUTE);
        list.add(OBJECTCOLLECTEXECUTEIMMEDIATELY);
        list.add(COMPUTERRESOURCE);
        list.add(SINGLEJOB);
        list.add(UNLOADDISTRIBUTEDATA);
        list.add(UNLOADRECEIVEDATA);
        list.add(CDC_EXECUTE);
        list.add(CDC_ABORT);
        list.add(CDC_STATUS);
        list.add(START_CDC_SYNC_PROGRAM);
        list.add(START_CDC_COLLECT_PROGRAM);
        list.add(CDC_COLLECT_GETPARAM);
        list.add(CDC_SYNC_GETPARAM);
        list.add(CDC_COLLECT_RUNSTATE);
        list.add(CDC_SYNC_RUNSTATE);
        list.add(CDC_COLLECT_FAILEDSTATE);
        list.add(CDC_SYNC_FAILEDSTATE);
        list.add(CDC_ADDDATASTOREREG);
    }

    private AgentActionUtil() {
    }

    public static String getUrl(long agent_id, long user_id, String methodName) {
        if (!list.contains(methodName)) {
            throw new BusinessException("被调用的agent接口" + methodName + "没有登记");
        }
        try {
            AgentDownInfo agent_down_info = Dbo.queryOneObject(AgentDownInfo.class, "SELECT distinct t1.agent_ip,t1.agent_port,t1.agent_context,t1.agent_pattern" + " FROM " + AgentDownInfo.TableName + " t1" + " JOIN " + AgentInfo.TableName + " t2 ON t1.agent_ip = t2.agent_ip" + " AND t1.agent_port = t2.agent_port" + " WHERE  t2.agent_id= ? AND t2.user_id = ?", agent_id, user_id).orElseThrow(() -> new BusinessException("根据Agent_id:" + agent_id + "查询不到部署信息"));
            String agentPattern = agent_down_info.getAgent_pattern();
            Validator.notBlank(agentPattern, "agent的访问路径不能为空");
            if (StringUtil.isNotBlank(agentPattern) && agentPattern.endsWith("/*")) {
                agent_down_info.setAgent_pattern(agentPattern.substring(0, agentPattern.length() - 2));
            }
            String agentContext = agent_down_info.getAgent_context();
            Validator.notBlank(agentContext, "agent的context不能为空");
            if (agentContext.endsWith("/")) {
                agent_down_info.setAgent_context(agentContext.substring(0, agentContext.length() - 1));
            }
            return "http://" + agent_down_info.getAgent_ip() + ":" + agent_down_info.getAgent_port() + agentContext + methodName;
        } catch (Exception e) {
            throw new BusinessException(String.format("根据agent_id、用户id和方法全路径获取访问远端Agent的接口全路径失败：%s", e));
        }
    }

    public static String getServerUrl(String methodName) {
        if (!list.contains(methodName)) {
            throw new AppSystemException(String.format("被调用的agent接口%s没有登记:", methodName));
        }
        String httpActionPattern = HttpServerConf.MANGAGEMENT_ACTION_PATTERN;
        String httpContextPath = HttpServerConf.MANGAGEMENT_CONTEXT_PATH;
        String httpAddress = HttpServerConf.MANGAGEMENT_ADDRESS;
        String httpPort = HttpServerConf.MANGAGEMENT_PORT;
        if (StringUtil.isNotBlank(httpActionPattern)) {
            if (httpActionPattern.endsWith("/*")) {
                httpActionPattern = httpActionPattern.substring(0, httpActionPattern.length() - 2);
            }
            if (httpActionPattern.endsWith("/")) {
                httpActionPattern = httpActionPattern.substring(0, httpActionPattern.length() - 1);
            }
            return "http://" + httpAddress + ":" + httpPort + httpContextPath + httpActionPattern + methodName;
        } else {
            return "http://" + httpAddress + ":" + httpPort + httpContextPath + methodName;
        }
    }
}
