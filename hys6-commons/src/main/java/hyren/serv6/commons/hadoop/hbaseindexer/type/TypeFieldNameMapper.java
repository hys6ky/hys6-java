package hyren.serv6.commons.hadoop.hbaseindexer.type;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.hadoop.hbaseindexer.bean.HbaseSolrField;

public class TypeFieldNameMapper implements FieldNameMapper {

    public static final String STRING_TYPE_SUFFIX = "F-";

    public static final String LONG_TYPE_SUFFIX = "FL-";

    public static final String BOOLEAN_TYPE_SUFFIX = "FB-";

    public static final String FLOAT_TYPE_SUFFIX = "FF-";

    public static final String DOUBLE_TYPE_SUFFIX = "FD-";

    public static final String DATE_TYPE_SUFFIX = "FDT-";

    public static final String INT_TYPE_SUFFIX = "FI-";

    @Override
    public void map(HbaseSolrField hsf) {
        String type = hsf.getType();
        String hbaseFieldName = hsf.getHbaseColumnName();
        if (StringUtil.isBlank(type) || StringUtil.isBlank(hbaseFieldName)) {
            throw new BusinessException("Invalid object: " + hsf.toString());
        }
        if (type.equalsIgnoreCase(SolrTypeEnum.STRING.toString())) {
            hsf.setSolrColumnName(STRING_TYPE_SUFFIX + hbaseFieldName);
        } else if (type.equalsIgnoreCase(SolrTypeEnum.LONG.toString())) {
            hsf.setSolrColumnName(LONG_TYPE_SUFFIX + hbaseFieldName);
        } else if (type.equalsIgnoreCase(SolrTypeEnum.BOOLEAN.toString())) {
            hsf.setSolrColumnName(BOOLEAN_TYPE_SUFFIX + hbaseFieldName);
        } else if (type.equalsIgnoreCase(SolrTypeEnum.FLOAT.toString())) {
            hsf.setSolrColumnName(FLOAT_TYPE_SUFFIX + hbaseFieldName);
        } else if (type.equalsIgnoreCase(SolrTypeEnum.DOUBLE.toString())) {
            hsf.setSolrColumnName(DOUBLE_TYPE_SUFFIX + hbaseFieldName);
        } else if (type.equalsIgnoreCase(SolrTypeEnum.DATE.toString())) {
            hsf.setSolrColumnName(DATE_TYPE_SUFFIX + hbaseFieldName);
        } else if (type.equalsIgnoreCase(SolrTypeEnum.INT.toString())) {
            hsf.setSolrColumnName(INT_TYPE_SUFFIX + hbaseFieldName);
        } else {
            throw new BusinessException("Can not recongnize field type : " + type);
        }
    }
}
