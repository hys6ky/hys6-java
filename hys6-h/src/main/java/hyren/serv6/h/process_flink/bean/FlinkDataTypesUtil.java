package hyren.serv6.h.process_flink.bean;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class FlinkDataTypesUtil {

    public static DataType createDataType(String dataTypeString) {
        int indexOf = dataTypeString.indexOf("(");
        if (indexOf > 0) {
            dataTypeString = dataTypeString.substring(0, indexOf);
        }
        switch(dataTypeString.toUpperCase()) {
            case "INT8":
            case "INTEGER":
            case "NUMBER":
            case "BIGINT":
                return DataTypes.BIGINT();
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "BYTES":
                return DataTypes.BYTES();
            case "DATE":
                return DataTypes.DATE();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "INT":
                return DataTypes.INT();
            case "NULL":
                return DataTypes.NULL();
            case "ROW":
                return DataTypes.ROW();
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "BPCHAR":
            case "CHAR":
            case "VARCHAR":
            case "STRING":
                return DataTypes.STRING();
            case "TIME":
                return DataTypes.TIME();
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();
            case "TIMESTAMP_LTZ":
                return DataTypes.TIMESTAMP_LTZ();
            case "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
            case "TIMESTAMP_WITH_TIME_ZONE":
                return DataTypes.TIMESTAMP_WITH_TIME_ZONE();
            case "TINYINT":
                return DataTypes.TINYINT();
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataTypeString);
        }
    }
}
