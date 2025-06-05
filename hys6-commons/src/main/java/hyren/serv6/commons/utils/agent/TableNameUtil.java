package hyren.serv6.commons.utils.agent;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.codes.StorageType;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2022/01/19 10:43")
public class TableNameUtil {

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String getTempTableNameSuffixT(String tableName) {
        return tableName.concat("t");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String getUnderline1TableName(String tableName) {
        return tableName.concat("_1");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Param(name = "storage_type", desc = "", range = "")
    @Param(name = "storage_time", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String getUnderline1TableName(String tableName, String storage_type, Long storage_time) {
        if ((StorageType.TiHuan == StorageType.ofEnumByCode(storage_type) || StorageType.ZhuiJia == StorageType.ofEnumByCode(storage_type)) && storage_time == 0) {
            return tableName;
        }
        return tableName.concat("_1");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String getBackupTableNameSuffixB(String tableName) {
        return tableName.concat("b");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String getUnderline1bTableName(String tableName) {
        return tableName.concat("_1b");
    }

    private static final char TABLE_SUFFIX_SYMBOL = '_';

    public static String getSpliceTableName(String tableName, long index) {
        return tableName + TABLE_SUFFIX_SYMBOL + index;
    }
}
