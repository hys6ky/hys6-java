package hyren.serv6.hadoop.commons.hbaseindexer.process;

import hyren.serv6.commons.hadoop.hbaseindexer.bean.HbaseSolrField;
import hyren.serv6.hadoop.commons.hbaseindexer.configure.ConfigurationUtil;
import hyren.serv6.commons.hadoop.hbaseindexer.type.DataTypeTransformSolr;
import hyren.serv6.commons.utils.constant.Constant;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XMLWriter {

    private static final String UNIQUE_KEY_FORMATTER = "customization.hyren.formatter.UniqueTableKeyFormatterImpl";

    private static final String CF = new String(Constant.HBASE_COLUMN_FAMILY);

    public static void write(String hbaseTableName, String xmlFilePath, List<HbaseSolrField> fields) throws IOException {
        List<String> xmlContents = new ArrayList<>();
        xmlContents.add("<?xml version=\"1.0\"?>");
        xmlContents.add("<indexer table=\"" + hbaseTableName + "\" mapping-type=\"row\" read-row=\"never\" table-name-field=\"" + ConfigurationUtil.TABLE_NAME_FIELD + "\" unique-key-formatter=\"" + UNIQUE_KEY_FORMATTER + "\" >");
        for (HbaseSolrField field : fields) {
            xmlContents.add("<field name=\"" + field.getSolrColumnName() + "\" value=\"" + CF + ":" + field.getHbaseColumnName() + "\" source=\"value\" type=\"" + DataTypeTransformSolr.transformToRealTypeInSolr(field.getType()) + "\"/>");
        }
        xmlContents.add("</indexer>");
        FileUtils.writeLines(new File(xmlFilePath), xmlContents);
    }
}
