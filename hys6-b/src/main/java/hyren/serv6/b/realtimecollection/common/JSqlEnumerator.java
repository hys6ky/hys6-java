package hyren.serv6.b.realtimecollection.common;

import hyren.serv6.b.realtimecollection.util.JSqlDateUtils;
import org.apache.calcite.linq4j.Enumerator;
import java.math.BigDecimal;
import java.util.List;

public class JSqlEnumerator<E> implements Enumerator<E> {

    private int currentIndex = -1;

    private List<String> columnTypes;

    private List<List<String>> data;

    private RowConverter<E> rowConvert;

    @SuppressWarnings("unchecked")
    public JSqlEnumerator(int[] fields, List<String> types, List<List<String>> data) {
        this.data = data;
        this.columnTypes = types;
        rowConvert = (RowConverter<E>) new ArrayRowConverter(fields);
    }

    abstract static class RowConverter<E> {

        abstract E convertRow(List<String> rows, List<String> columnTypes);
    }

    static class ArrayRowConverter extends RowConverter<Object[]> {

        private int[] fields;

        public ArrayRowConverter(int[] fields) {
            this.fields = fields;
        }

        @Override
        Object[] convertRow(List<String> rows, List<String> columnTypes) {
            Object[] objects = new Object[fields.length];
            int i = 0;
            for (int field : this.fields) {
                objects[i++] = convertOptiqCellValue(rows.get(field), columnTypes.get(field));
            }
            return objects;
        }
    }

    public void close() {
    }

    public static Object convertOptiqCellValue(String strValue, String dataType) {
        if (strValue == null)
            return null;
        if ((strValue.equals("") || strValue.equals("\\N")) && !dataType.equals("string"))
            return null;
        if ("date".equals(dataType)) {
            return JSqlDateUtils.stringToDate(strValue);
        } else if ("tinyint".equals(dataType)) {
            return Byte.valueOf(strValue);
        } else if ("short".equals(dataType) || "smallint".equals(dataType)) {
            return Short.valueOf(strValue);
        } else if ("integer".equals(dataType)) {
            return Integer.valueOf(strValue);
        } else if ("long".equals(dataType) || "bigint".equals(dataType)) {
            return Long.valueOf(strValue);
        } else if ("double".equals(dataType)) {
            return Double.valueOf(strValue);
        } else if ("decimal".equals(dataType)) {
            return new BigDecimal(strValue);
        } else if ("timestamp".equals(dataType)) {
            return JSqlDateUtils.stringToMillis(strValue);
        } else if ("float".equals(dataType)) {
            return Float.valueOf(strValue);
        } else if ("boolean".equals(dataType)) {
            return Boolean.valueOf(strValue);
        } else {
            return strValue;
        }
    }

    public E current() {
        List<String> line = data.get(currentIndex);
        return rowConvert.convertRow(line, this.columnTypes);
    }

    public boolean moveNext() {
        return ++currentIndex < data.size();
    }

    public void reset() {
    }
}
