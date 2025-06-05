package hyren.serv6.g.init;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.codes.MenuType;
import hyren.serv6.base.entity.*;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.g.bean.QueryInterfaceInfo;
import hyren.serv6.g.bean.TokenModel;
import hyren.serv6.g.commons.TokenManagerImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@DocClass(desc = "", author = "dhw", createdate = "2020/3/30 16:09")
@Component
public class InterfaceManager {

    private static final Logger logger = LogManager.getLogger();

    private static final ConcurrentHashMap<Long, QueryInterfaceInfo> userMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, QueryInterfaceInfo> interfaceMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, QueryInterfaceInfo> tableMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, QueryInterfaceInfo> toKenMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, List<String>> urlMap = new ConcurrentHashMap<>();

    private static final Set<Long> userSet = ConcurrentHashMap.newKeySet();

    private static final ConcurrentHashMap<Long, List<String>> reportGraphicMap = new ConcurrentHashMap<>();

    static {
        logger.info("初始化接口开始");
        initAll();
    }

    @Method(desc = "", logicStep = "")
    public static void initAll() {
        logger.info("初始化接口所有信息开始。。。。。。。。。");
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            userInfo(db);
            userInterface(db);
            userTableInfo(db);
            db.commit();
        }
        logger.info("初始化接口所有信息结束。。。。。。。。。");
    }

    @Method(desc = "", logicStep = "")
    public static void initUser(DatabaseWrapper db) {
        userInfo(db);
        db.commit();
    }

    @Method(desc = "", logicStep = "")
    public static void initInterface(DatabaseWrapper db) {
        userInterface(db);
    }

    @Method(desc = "", logicStep = "")
    public static void initTable(DatabaseWrapper db) {
        userTableInfo(db);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static QueryInterfaceInfo getUserTokenInfo(DatabaseWrapper db, Long user_id) {
        if (!userMap.containsKey(user_id)) {
            initUser(db);
        }
        return userMap.get(user_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "url", desc = "", range = "")
    @Return(desc = "", range = "")
    public static QueryInterfaceInfo getInterfaceUseInfo(Long user_id, String url) {
        return interfaceMap.get(String.valueOf(user_id).concat(url));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<String> getUrlInfo(String user_id) {
        return urlMap.get(user_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static QueryInterfaceInfo getUserTableInfo(DatabaseWrapper db, Long user_id, String tableName) {
        String key = String.valueOf(user_id).concat(tableName.toUpperCase());
        if (!tableMap.containsKey(key)) {
            initTable(db);
        }
        return tableMap.get(key);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "toKen", desc = "", range = "")
    @Return(desc = "", range = "")
    public static QueryInterfaceInfo getUserByToken(String toKen) {
        return toKenMap.get(toKen);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getTableList(Long user_id) {
        List<Map<String, Object>> list = new ArrayList<>();
        tableMap.forEach((key, value) -> {
            Map<String, Object> map;
            if (key.contains(String.valueOf(user_id))) {
                map = new HashMap<>();
                map.put("SYSREG_NAME", value.getSysreg_name());
                map.put("ORIGINAL_NAME", value.getOriginal_name());
                map.put("TABLE_BLSYSTEM", value.getTable_blsystem());
                list.add(map);
            }
        });
        return list;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "url", desc = "", range = "")
    public static void removeUserInterInfo(String user_id, String url) {
        interfaceMap.remove(String.valueOf(user_id).concat(url));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    public static void removeUserTableInfo(String user_id, String tableName) {
        tableMap.remove(String.valueOf(user_id).concat(tableName.toUpperCase()));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "toKen", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean existsToken(DatabaseWrapper db, String toKen) {
        if (!toKenMap.containsKey(toKen)) {
            initUser(db);
        }
        return toKenMap.containsKey(toKen);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "interface_code", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean existsReportGraphic(Long user_id, String interface_code) {
        return reportGraphicMap.get(user_id).contains(interface_code);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean existsTable(DatabaseWrapper db, Long user_id, String tableName) {
        String tableKey = String.valueOf(user_id).concat(tableName.toUpperCase());
        if (!tableMap.containsKey(tableKey)) {
            initUser(db);
            initTable(db);
        }
        return tableMap.containsKey(tableKey);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "url", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean existsInterface(DatabaseWrapper db, Long user_id, String url) {
        String interfaceKey = String.valueOf(user_id).concat(url);
        if (!interfaceMap.containsKey(interfaceKey)) {
            initUser(db);
            initInterface(db);
        }
        return interfaceMap.containsKey(interfaceKey);
    }

    @Method(desc = "", logicStep = "")
    private static void userInfo(DatabaseWrapper db) {
        Result userResult = SqlOperator.queryResult(db, "SELECT t1.user_id,t1.user_password,t1.user_name,t1.valid_time FROM " + SysUser.TableName + " t1 JOIN " + RoleMenu.TableName + " t2 ON t1.role_id = t2.role_id JOIN " + ComponentMenu.TableName + " t3 ON t2.menu_id = t3.menu_id " + " WHERE t3.menu_type = ?", MenuType.CaoZhuoYuan.getCode());
        if (!userResult.isEmpty()) {
            userSet.clear();
            toKenMap.clear();
            userMap.clear();
            QueryInterfaceInfo queryInterfaceInfo;
            TokenManagerImpl tokenManager = new TokenManagerImpl();
            for (int i = 0; i < userResult.getRowCount(); i++) {
                queryInterfaceInfo = new QueryInterfaceInfo();
                Long user_id = userResult.getLong(i, "user_id");
                queryInterfaceInfo.setUser_id(user_id);
                queryInterfaceInfo.setUser_name(userResult.getString(i, "user_name"));
                String user_password = userResult.getString(i, "user_password");
                queryInterfaceInfo.setUse_valid_date(userResult.getString(i, "valid_time"));
                if (user_password.endsWith("==")) {
                    queryInterfaceInfo.setUser_password(new String(Base64.getDecoder().decode(user_password)));
                } else {
                    queryInterfaceInfo.setUser_password(user_password);
                }
                TokenModel createToken = tokenManager.createToken(db, user_id, user_password);
                queryInterfaceInfo.setToken(createToken.getToken());
                userSet.add(user_id);
                toKenMap.put(createToken.getToken(), queryInterfaceInfo);
                userMap.put(user_id, queryInterfaceInfo);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    private static void userInterface(DatabaseWrapper db) {
        StringBuilder sql = new StringBuilder();
        sql.append("select url,start_use_date,use_valid_date,user_id,use_state,interface_code," + "interface_name,interface_id,interface_use_id from " + InterfaceUse.TableName).append(" where user_id in (");
        userSet.forEach(user -> sql.append(user).append(","));
        sql.delete(sql.length() - 1, sql.length());
        sql.append(" )");
        Result useResult = SqlOperator.queryResult(db, sql.toString());
        if (!useResult.isEmpty()) {
            reportGraphicMap.clear();
            interfaceMap.clear();
            urlMap.clear();
            QueryInterfaceInfo queryInterfaceInfo;
            List<String> interfaceCodeList;
            List<String> urlList;
            for (int i = 0; i < useResult.getRowCount(); i++) {
                queryInterfaceInfo = new QueryInterfaceInfo();
                Long user_id = useResult.getLong(i, "user_id");
                queryInterfaceInfo.setUser_id(user_id);
                String url = useResult.getString(i, "url");
                queryInterfaceInfo.setUrl(url);
                queryInterfaceInfo.setStart_use_date(useResult.getString(i, "start_use_date"));
                queryInterfaceInfo.setUse_valid_date(useResult.getString(i, "use_valid_date"));
                queryInterfaceInfo.setUse_state(useResult.getString(i, "use_state"));
                queryInterfaceInfo.setInterface_name(useResult.getString(i, "interface_name"));
                queryInterfaceInfo.setInterface_id(useResult.getString(i, "interface_id"));
                queryInterfaceInfo.setInterface_use_id(useResult.getString(i, "interface_use_id"));
                String interface_code = useResult.getString(i, "interface_code");
                if (reportGraphicMap.containsKey(user_id)) {
                    reportGraphicMap.get(user_id).add(interface_code);
                } else {
                    interfaceCodeList = new ArrayList<>();
                    interfaceCodeList.add(interface_code);
                    reportGraphicMap.put(user_id, interfaceCodeList);
                }
                interfaceMap.put(String.valueOf(user_id).concat(url), queryInterfaceInfo);
                if (urlMap.containsKey(String.valueOf(user_id))) {
                    urlMap.get(String.valueOf(user_id)).add(url);
                } else {
                    urlList = new ArrayList<>();
                    urlList.add(url);
                    urlMap.put(String.valueOf(user_id), urlList);
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    private static void userTableInfo(DatabaseWrapper db) {
        StringBuilder sql = new StringBuilder();
        sql.append("select * from " + TableUseInfo.TableName + " where user_id in (");
        userSet.forEach(user -> sql.append(user).append(","));
        sql.delete(sql.length() - 1, sql.length());
        sql.append(" )");
        Result tableResult = SqlOperator.queryResult(db, sql.toString());
        if (!tableResult.isEmpty()) {
            tableMap.clear();
            QueryInterfaceInfo queryInterfaceInfo;
            for (int i = 0; i < tableResult.getRowCount(); i++) {
                queryInterfaceInfo = new QueryInterfaceInfo();
                Long user_id = tableResult.getLong(i, "user_id");
                queryInterfaceInfo.setUser_id(user_id);
                String sysreg_name = tableResult.getString(i, "sysreg_name");
                long use_id = tableResult.getLong(i, "use_id");
                queryInterfaceInfo.setSysreg_name(sysreg_name);
                List<SysregParameterInfo> parameterInfos = SqlOperator.queryList(db, SysregParameterInfo.class, "select * from " + SysregParameterInfo.TableName + " where use_id=? and user_id=?", use_id, user_id);
                StringBuilder chColumns = new StringBuilder();
                StringBuilder enColumns = new StringBuilder();
                StringBuilder remarks = new StringBuilder();
                if (parameterInfos != null && parameterInfos.size() > 0) {
                    for (SysregParameterInfo parameterInfo : parameterInfos) {
                        chColumns.append(parameterInfo.getTable_ch_column()).append(Constant.METAINFOSPLIT);
                        enColumns.append(parameterInfo.getTable_en_column()).append(Constant.METAINFOSPLIT);
                        remarks.append(parameterInfo.getRemark()).append(Constant.METAINFOSPLIT);
                    }
                    queryInterfaceInfo.setTable_ch_column(chColumns.deleteCharAt(chColumns.length() - 1).toString());
                    queryInterfaceInfo.setTable_en_column(enColumns.deleteCharAt(enColumns.length() - 1).toString());
                    queryInterfaceInfo.setTable_type_name(remarks.deleteCharAt(remarks.length() - 1).toString());
                } else {
                    queryInterfaceInfo.setTable_ch_column("");
                    queryInterfaceInfo.setTable_en_column("");
                }
                queryInterfaceInfo.setOriginal_name(tableResult.getString(i, "original_name"));
                queryInterfaceInfo.setTable_blsystem(tableResult.getString(i, "table_blsystem").trim());
                queryInterfaceInfo.setUse_id(tableResult.getString(i, "use_id"));
                tableMap.put(String.valueOf(user_id).concat(sysreg_name.toUpperCase()), queryInterfaceInfo);
            }
        }
    }
}
