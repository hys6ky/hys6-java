package hyren.serv6.hadoop.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.example.data.Group;

public class ColUtil {

    public void addData2Group(Group group, String columnType, String columname, String data) {
        if (columnType.contains("BOOLEAN")) {
            boolean dataResult = Boolean.valueOf(data.trim());
            group.add(columname, dataResult);
        } else if (columnType.contains("INT")) {
            int dataResult = StringUtils.isEmpty(data) ? 0 : Integer.valueOf(data.trim());
            group.add(columname, dataResult);
        } else if (columnType.contains("FLOAT")) {
            float dataResult = StringUtils.isEmpty(data) ? 0 : Float.valueOf(data.trim());
            group.add(columname, dataResult);
        } else if (columnType.contains("DOUBLE") || columnType.contains("DECIMAL")) {
            double dataResult = StringUtils.isEmpty(data) ? 0 : Double.valueOf(data.trim());
            group.add(columname, dataResult);
        } else {
            data = StringUtils.isEmpty(data) ? "" : data;
            group.add(columname, data);
        }
    }
}
