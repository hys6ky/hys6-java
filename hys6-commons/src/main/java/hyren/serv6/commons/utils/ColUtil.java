package hyren.serv6.commons.utils;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import org.apache.commons.lang3.StringUtils;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ColUtil {

    public void addData2Inspector(List<Object> lineData, String columnType, String data) {
        data = data.trim();
        if (columnType.contains("BOOLEAN")) {
            boolean dataResult = Boolean.valueOf(data);
            lineData.add(dataResult);
        } else if (columnType.contains("INT")) {
            int dataResult = StringUtils.isEmpty(data) ? 0 : Integer.valueOf(data);
            lineData.add(dataResult);
        } else if (columnType.contains("FLOAT")) {
            float dataResult = StringUtils.isEmpty(data) ? 0 : Float.valueOf(data);
            lineData.add(dataResult);
        } else if (columnType.contains("DOUBLE")) {
            double dataResult = StringUtils.isEmpty(data) ? 0 : Double.valueOf(data);
            lineData.add(dataResult);
        } else if (columnType.contains("DECIMAL")) {
            BigDecimal dataResult = StringUtils.isEmpty(data) ? new BigDecimal("0") : new BigDecimal(data);
            lineData.add(dataResult);
            ClassBase.hadoopInstance().createHiveDecimal(dataResult, lineData);
        } else {
            data = StringUtils.isEmpty(data) ? "" : data;
            lineData.add(data);
        }
    }

    public static List<String> getTarTypes(TableBean tableBean, long dsl_id) {
        Map<Long, String> tbColTarMap = tableBean.getTbColTarMap();
        String tbColTarType = tbColTarMap.get(dsl_id);
        List<String> tarTypes = new ArrayList<>();
        if (StringUtil.isNotBlank(tbColTarType)) {
            tarTypes = StringUtil.split(tbColTarType, Constant.METAINFOSPLIT);
        }
        return tarTypes;
    }
}
