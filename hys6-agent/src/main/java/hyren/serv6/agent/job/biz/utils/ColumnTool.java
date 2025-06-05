package hyren.serv6.agent.job.biz.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.agent.bean.CollectTableColumnBean;
import hyren.serv6.commons.utils.agent.constant.DataTypeConstant;
import hyren.serv6.commons.utils.constant.Constant;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@DocClass(desc = "", author = "WangZhengcheng")
public class ColumnTool {

    @Method(desc = "", logicStep = "")
    @Param(name = "list", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Set<String> getCollectColumnName(List<CollectTableColumnBean> columnList) {
        if (columnList == null || columnList.isEmpty()) {
            throw new AppSystemException("采集作业信息不能为空");
        }
        Set<String> columnNames = new HashSet<>();
        for (CollectTableColumnBean column : columnList) {
            if (IsFlag.Fou.getCode().equals(column.getIs_new())) {
                String columnName = column.getColumn_name();
                if (!(Constant._HYREN_S_DATE.equalsIgnoreCase(columnName) || Constant._HYREN_E_DATE.equalsIgnoreCase(columnName) || Constant._HYREN_MD5_VAL.equalsIgnoreCase(columnName) || Constant._HYREN_OPER_DATE.equalsIgnoreCase(columnName) || Constant._HYREN_OPER_TIME.equalsIgnoreCase(columnName) || Constant._HYREN_OPER_PERSON.equalsIgnoreCase(columnName))) {
                    columnNames.add(columnName);
                }
            }
        }
        return columnNames;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnsName", desc = "", range = "")
    @Param(name = "colName", desc = "", range = "")
    @Param(name = "separator", desc = "", range = "")
    @Return(desc = "", range = "")
    public static int findColIndex(String columnsName, String colName, String separator) {
        List<String> column = StringUtil.split(columnsName, separator);
        int index = 0;
        for (int j = 0; j < column.size(); j++) {
            if (column.get(j).equalsIgnoreCase(colName)) {
                index = j + 1;
                break;
            }
        }
        return index;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnsTypeAndPreci", desc = "", range = "")
    @Param(name = "index", desc = "", range = "")
    @Param(name = "separator", desc = "", range = "")
    @Return(desc = "", range = "")
    public static int searchIndex(String columnsTypeAndPreci, int index, String separator) {
        int temp = 0;
        int num = 0;
        while (temp != -1) {
            num++;
            temp = columnsTypeAndPreci.indexOf(separator, temp + 1);
            if (num == index) {
                break;
            }
        }
        return temp;
    }

    public static void addData2Inspector(List<Object> lineData, String columnType, String data) {
        columnType = columnType.toLowerCase();
        if (columnType.contains(DataTypeConstant.BOOLEAN.getMessage())) {
            boolean dataResult = !StringUtil.isEmpty(data) && Boolean.parseBoolean(data.trim());
            lineData.add(dataResult);
        } else if (columnType.contains(DataTypeConstant.INT8.getMessage()) || columnType.equals(DataTypeConstant.BIGINT.getMessage()) || columnType.equals(DataTypeConstant.LONG.getMessage())) {
            long dataResult = StringUtil.isEmpty(data) ? 0L : Long.parseLong(data.trim());
            lineData.add(dataResult);
        } else if (columnType.contains(DataTypeConstant.INT.getMessage())) {
            int dataResult = StringUtil.isEmpty(data) ? 0 : Integer.parseInt(data.trim());
            lineData.add(dataResult);
        } else if (columnType.contains(DataTypeConstant.FLOAT.getMessage())) {
            float dataResult = StringUtil.isEmpty(data) ? Float.valueOf("0") : Float.valueOf(data.trim());
            lineData.add(dataResult);
        } else if (columnType.contains(DataTypeConstant.DOUBLE.getMessage())) {
            double dataResult = StringUtil.isEmpty(data) ? Double.valueOf("0") : Double.valueOf(data.trim());
            lineData.add(dataResult);
        } else if (columnType.contains(DataTypeConstant.DECIMAL.getMessage()) || columnType.contains(DataTypeConstant.NUMERIC.getMessage())) {
            BigDecimal dataResult = StringUtil.isEmpty(data) ? new BigDecimal("0") : new BigDecimal(data.trim());
            ClassBase.hadoopInstance().createHiveDecimal(dataResult, lineData);
        } else {
            data = StringUtil.isEmpty(data) ? "" : data;
            lineData.add(data);
        }
    }
}
