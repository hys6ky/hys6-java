package hyren.serv6.commons.utils.xlstoxml;

import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.xlstoxml.util.SignalParser;
import lombok.extern.slf4j.Slf4j;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class Signal2xml {

    private Signal2xml() {
    }

    public static boolean isColChange(String signal_path, String xml_path) throws Exception {
        SignalParser parser = new SignalParser(signal_path);
        String table_name = parser.getFilename().substring(0, parser.getFilename().lastIndexOf("_")).toLowerCase();
        List<String> old_cols = new ArrayList<>();
        List<String> new_cols = new ArrayList<>();
        File xml_file = new File(xml_path);
        if (xml_file.exists()) {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xml_file);
            NodeList table_list = doc.getElementsByTagName("table");
            for (int i = 0, tables_len = table_list.getLength(); i < tables_len; i++) {
                Node table = table_list.item(i);
                String name = table.getAttributes().getNamedItem("name").getNodeValue().toLowerCase();
                if (table_name.equalsIgnoreCase(name)) {
                    NodeList column_list = table.getChildNodes();
                    for (int j = 0, columns_len = column_list.getLength(); j < columns_len; j++) {
                        String col_name = column_list.item(j).getAttributes().getNamedItem("name").getNodeValue();
                        old_cols.add(col_name);
                    }
                }
            }
        }
        List<Map<String, Object>> columns = parser.getColumndescription();
        for (Map<String, Object> col : columns) {
            String col_name = col.get("name").toString();
            new_cols.add(col_name);
        }
        int old_cols_size = old_cols.size();
        if (old_cols_size > 0) {
            if (old_cols_size != new_cols.size()) {
                return true;
            } else {
                old_cols.retainAll(new_cols);
                return old_cols.size() != old_cols_size;
            }
        }
        return false;
    }

    public static void toXml(String signal_path, String xml_path) {
        DocumentBuilderFactory factory;
        DocumentBuilder builder;
        Document doc;
        Element root;
        NodeList table_list;
        File xml_file = new File(xml_path);
        XmlCreater xmlCreater = new XmlCreater(xml_file.getAbsolutePath());
        try {
            SignalParser parser = new SignalParser(signal_path);
            String table_name = parser.getFilename().substring(0, parser.getFilename().lastIndexOf("_")).toLowerCase();
            List<Map<String, Object>> columns = parser.getColumndescription();
            String storage_type = "1";
            if (xml_file.exists()) {
                factory = DocumentBuilderFactory.newInstance();
                builder = factory.newDocumentBuilder();
                doc = builder.parse(xml_file);
                root = doc.getDocumentElement();
                table_list = doc.getElementsByTagName("table");
                for (int i = 0; i < table_list.getLength(); i++) {
                    Node item = table_list.item(i);
                    String name = item.getAttributes().getNamedItem("name").getNodeValue().toLowerCase();
                    if (table_name.equals(name)) {
                        root.removeChild(item);
                    }
                }
                if (!xml_file.delete()) {
                    throw new BusinessException("删除文件失败!" + xml_file.getAbsolutePath());
                }
            } else {
                doc = xmlCreater.getDoc();
                root = xmlCreater.createRootElement("database");
                xmlCreater.createAttribute(root, "xmlns", "http://db.apache.org/ddlutils/schema/1.1");
                xmlCreater.createAttribute(root, "name", "dict_params");
            }
            Element table = xmlCreater.createElement(root, "table");
            xmlCreater.createAttribute(table, "name", table_name);
            xmlCreater.createAttribute(table, "description", table_name);
            xmlCreater.createAttribute(table, "storage_type", storage_type);
            for (Map<String, Object> col : columns) {
                int length = Integer.parseInt(col.get("end").toString()) - Integer.parseInt(col.get("start").toString()) + 1;
                Element column = xmlCreater.createElement(table, "column");
                xmlCreater.createAttribute(column, "column_id", col.get("index").toString());
                xmlCreater.createAttribute(column, "name", col.get("name").toString());
                xmlCreater.createAttribute(column, "column_cn_name", "");
                xmlCreater.createAttribute(column, "column_type", col.get("type").toString());
                xmlCreater.createAttribute(column, "length", String.valueOf(length));
                xmlCreater.createAttribute(column, "column_key", "");
                xmlCreater.createAttribute(column, "column_null", "");
                xmlCreater.createAttribute(column, "column_remark", "");
                xmlCreater.createAttribute(column, "primaryKey", "");
            }
            TransformerFactory tfactory = TransformerFactory.newInstance();
            Transformer transformer = tfactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(xml_file);
            transformer.setOutputProperty("encoding", "UTF-8");
            transformer.transform(source, result);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
