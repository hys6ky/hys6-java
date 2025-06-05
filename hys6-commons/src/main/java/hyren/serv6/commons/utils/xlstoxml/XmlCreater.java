package hyren.serv6.commons.utils.xlstoxml;

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

@Slf4j
public class XmlCreater {

    private Document doc = null;

    private String path;

    public XmlCreater(String path) {
        this.path = path;
        init();
    }

    private void init() {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            doc = builder.newDocument();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public Element createRootElement(String rootTagName) {
        if (doc.getDocumentElement() == null) {
            Element root = doc.createElement(rootTagName);
            doc.appendChild(root);
            return root;
        }
        return doc.getDocumentElement();
    }

    public void open() {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            doc = builder.parse(path);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public Element createElement(Element parent, String tagName) {
        Document doc = parent.getOwnerDocument();
        Element child = doc.createElement(tagName);
        parent.appendChild(child);
        return child;
    }

    public Element getElement() {
        return doc.getDocumentElement();
    }

    public Element createElement(Element parent, String tagName, String value) {
        Document doc = parent.getOwnerDocument();
        Element child = doc.createElement(tagName);
        XmlOper.setElementValue(child, value);
        parent.appendChild(child);
        return child;
    }

    public void removeElement(String taName) {
        Element root = getElement();
        NodeList table_list = doc.getElementsByTagName("table");
        for (int i = 0; i < table_list.getLength(); i++) {
            Node item = table_list.item(i);
            Node node = item.getAttributes().getNamedItem("table_name");
            if (node == null) {
                continue;
            }
            if (taName.equalsIgnoreCase(node.getNodeValue().toLowerCase())) {
                root.removeChild(item);
            }
        }
    }

    public void createAttribute(Element parent, String attrName, String attrValue) {
        XmlOper.setElementAttr(parent, attrName, attrValue);
    }

    public void removeAttribute(Element parent, String attrName) {
        XmlOper.removeElementAttr(parent, attrName);
    }

    public void buildXmlFile() {
        TransformerFactory tfactory = TransformerFactory.newInstance();
        try {
            Transformer transformer = tfactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File(path));
            transformer.setOutputProperty("encoding", "UTF-8");
            transformer.transform(source, result);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public Document getDoc() {
        return doc;
    }

    public void setDoc(Document doc) {
        this.doc = doc;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
