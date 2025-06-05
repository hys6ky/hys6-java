package hyren.serv6.base.datatree.background.query;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.datatree.util.TreeConstant;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.packutil.PackageUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/1/7 0007 上午 11:17")
public class SFLDataQuery {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getSFLDataInfos() {
        List<Map<String, Object>> sflDataInfos = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        map.put("id", TreeConstant.SYS_DATA_TABLE);
        map.put("label", "系统数据表");
        map.put("parent_id", DataSourceType.SFL.getCode());
        map.put("description", "系统数据表");
        map.put("data_layer", DataSourceType.SFL.getCode());
        sflDataInfos.add(map);
        map = new HashMap<>();
        map.put("id", TreeConstant.SYS_DATA_BAK);
        map.put("label", "系统数据备份");
        map.put("parent_id", DataSourceType.SFL.getCode());
        map.put("description", "系统数据表");
        map.put("data_layer", DataSourceType.SFL.getCode());
        sflDataInfos.add(map);
        return sflDataInfos;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<String> getSFLTableInfos() {
        String packageName = "hrds.commons.entity";
        try {
            List<String> classNames = PackageUtil.getClassName(packageName, false);
            List<String> tableInfo = new ArrayList<>();
            if (null != classNames && !classNames.isEmpty()) {
                for (String className : classNames) {
                    tableInfo.add(StringUtil.replace(className, packageName + ".", "").toLowerCase());
                }
            }
            return tableInfo;
        } catch (Exception e) {
            throw new BusinessException("获取系统表名失败！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getSFLDataBakInfos() {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, "SELECT * from sys_dump");
        }
    }
}
