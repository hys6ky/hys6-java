package hyren.serv6.hadoop.utils;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.commons.utils.agent.constant.DataTypeConstant;
import hyren.serv6.commons.utils.constant.Constant;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.parquet.example.data.Group;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class ColumnTool {

    public static StructObjectInspector schemaInfo(String columns, String types) {
        List<ObjectInspector> listType = new ArrayList<>();
        List<String> listColumn = new ArrayList<>();
        List<String> columnAll = StringUtil.split(columns, Constant.METAINFOSPLIT);
        List<String> split = StringUtil.split(types, Constant.METAINFOSPLIT);
        for (int i = 0; i < columnAll.size(); i++) {
            String columns_type = split.get(i).toLowerCase();
            if (columns_type.contains(DataTypeConstant.BOOLEAN.getMessage())) {
                listType.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
            } else if (columns_type.contains(DataTypeConstant.INT8.getMessage()) || columns_type.equals(DataTypeConstant.BIGINT.getMessage()) || columns_type.equals(DataTypeConstant.LONG.getMessage())) {
                listType.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
            } else if (columns_type.contains(DataTypeConstant.INT.getMessage())) {
                listType.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            } else if (columns_type.contains(DataTypeConstant.CHAR.getMessage())) {
                listType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            } else if (columns_type.contains(DataTypeConstant.DECIMAL.getMessage()) || columns_type.contains(DataTypeConstant.NUMERIC.getMessage())) {
                listType.add(PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector);
            } else if (columns_type.contains(DataTypeConstant.FLOAT.getMessage())) {
                listType.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
            } else if (columns_type.contains(DataTypeConstant.DOUBLE.getMessage())) {
                listType.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
            } else {
                listType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            }
            listColumn.add(columnAll.get(i));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(listColumn, listType);
    }

    public static void addData2Group(Group group, String columnType, String columname, String data) {
        columnType = columnType.toLowerCase();
        if (columnType.contains(DataTypeConstant.BOOLEAN.getMessage())) {
            boolean dataResult = !StringUtil.isEmpty(data) && Boolean.parseBoolean(data.trim());
            group.add(columname, dataResult);
        } else if (columnType.contains(DataTypeConstant.INT8.getMessage()) || columnType.equals(DataTypeConstant.BIGINT.getMessage()) || columnType.equals(DataTypeConstant.LONG.getMessage())) {
            long dataResult = StringUtil.isEmpty(data) ? 0L : Long.parseLong(data.trim());
            group.add(columname, dataResult);
        } else if (columnType.contains(DataTypeConstant.INT.getMessage())) {
            int dataResult = StringUtil.isEmpty(data) ? 0 : Integer.parseInt(data.trim());
            group.add(columname, dataResult);
        } else if (columnType.contains(DataTypeConstant.FLOAT.getMessage())) {
            float dataResult = StringUtil.isEmpty(data) ? Float.valueOf("0") : Float.valueOf(data.trim());
            group.add(columname, dataResult);
        } else if (columnType.contains(DataTypeConstant.DOUBLE.getMessage()) || columnType.contains(DataTypeConstant.DECIMAL.getMessage()) || columnType.contains(DataTypeConstant.NUMERIC.getMessage())) {
            double dataResult = StringUtil.isEmpty(data) ? Double.valueOf("0") : Double.valueOf(data.trim());
            group.add(columname, dataResult);
        } else {
            data = StringUtil.isEmpty(data) ? "" : data;
            group.add(columname, data);
        }
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
            HiveDecimal create = HiveDecimal.create(dataResult);
            lineData.add(create);
        } else {
            data = StringUtil.isEmpty(data) ? "" : data;
            lineData.add(data);
        }
    }
}
