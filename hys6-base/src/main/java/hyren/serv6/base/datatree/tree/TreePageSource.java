package hyren.serv6.base.datatree.tree;

import fd.ng.core.annotation.DocClass;
import hyren.serv6.base.codes.DataSourceType;
import java.util.*;

@DocClass(desc = "", author = "BY-HLL", createdate = "2019/12/24 0024 上午 10:05")
public class TreePageSource {

    public static final String WEB_SQL = "web_sql";

    public static final String MARKET = "market";

    public static final String INTERFACE = "interface";

    public static final String DATA_MANAGEMENT = "data_management";

    public static final String DATA_BENCHMARKING = "data_benchmarking";

    public static final String MARKET_VERSION_MANAGE = "market_version_manage";

    public static final String STREAM_MANAGE = "stream_manage";

    public static final List<String> treeSourceList = new ArrayList<>(Arrays.asList(WEB_SQL, MARKET, INTERFACE, DATA_MANAGEMENT, DATA_BENCHMARKING, MARKET_VERSION_MANAGE, STREAM_MANAGE));

    private static final DataSourceType[] DATA_MANAGEMENT_ARRAY = new DataSourceType[] { DataSourceType.DCL, DataSourceType.DML, DataSourceType.DQC, DataSourceType.UDL };

    private static final DataSourceType[] WEB_SQL_ARRAY = new DataSourceType[] { DataSourceType.DCL, DataSourceType.DML, DataSourceType.DQC, DataSourceType.UDL };

    private static final DataSourceType[] PROCESS_ARRAY = new DataSourceType[] { DataSourceType.DCL, DataSourceType.DML, DataSourceType.DQC, DataSourceType.UDL };

    private static final DataSourceType[] HIRE_ARRAY = new DataSourceType[] { DataSourceType.DCL, DataSourceType.DML, DataSourceType.DQC, DataSourceType.UDL };

    private static final DataSourceType[] DATA_BENCHMARKING_ARRAY = new DataSourceType[] { DataSourceType.DCL, DataSourceType.DML, DataSourceType.DQC, DataSourceType.UDL };

    private static final DataSourceType[] MARKET_VERSION_MANAGE_ARR = new DataSourceType[] { DataSourceType.DML };

    private static final DataSourceType[] STREAM_MANAGE_ARR = new DataSourceType[] { DataSourceType.DCL, DataSourceType.DML, DataSourceType.DQC, DataSourceType.UDL, DataSourceType.KFK };

    public static Map<String, DataSourceType[]> TREE_SOURCE = new HashMap<>();

    static {
        TREE_SOURCE.put(WEB_SQL, WEB_SQL_ARRAY);
        TREE_SOURCE.put(MARKET, PROCESS_ARRAY);
        TREE_SOURCE.put(INTERFACE, HIRE_ARRAY);
        TREE_SOURCE.put(DATA_MANAGEMENT, DATA_MANAGEMENT_ARRAY);
        TREE_SOURCE.put(DATA_BENCHMARKING, DATA_BENCHMARKING_ARRAY);
        TREE_SOURCE.put(MARKET_VERSION_MANAGE, MARKET_VERSION_MANAGE_ARR);
        TREE_SOURCE.put(STREAM_MANAGE, STREAM_MANAGE_ARR);
    }
}
