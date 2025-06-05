package hyren.serv6.commons.utils.xlstoxml;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import java.util.ArrayList;
import java.util.List;

public class XmlOper {

    private XmlOper() {
    }

    public static NodeList getNodeList(Element parent) {
        return parent.getChildNodes();
    }

    public static Element[] getElementsByName(Element parent, String name) {
        List<Node> resList = new ArrayList<>();
        NodeList nl = getNodeList(parent);
        for (int i = 0; i < nl.getLength(); i++) {
            Node nd = nl.item(i);
            if (nd.getNodeName().equals(name)) {
                resList.add(nd);
            }
        }
        Element[] res = new Element[resList.size()];
        for (Node node : resList) {
            res[0] = (Element) node;
        }
        return res;
    }

    public static String getElementName(Element element) {
        return element.getNodeName();
    }

    public static String getElementValue(Element element) {
        NodeList nl = element.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            if (nl.item(i).getNodeType() == Node.TEXT_NODE) {
                return element.getFirstChild().getNodeValue();
            }
        }
        return null;
    }

    public static String getElementAttr(Element element, String attr) {
        return element.getAttribute(attr);
    }

    public static void setElementValue(Element element, String val) {
        Node node = element.getOwnerDocument().createTextNode(val);
        NodeList nl = element.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node nd = nl.item(i);
            if (nd.getNodeType() == Node.TEXT_NODE) {
                nd.setNodeValue(val);
                return;
            }
        }
        element.appendChild(node);
    }

    public static void setElementAttr(Element element, String attr, String attrVal) {
        element.setAttribute(attr, attrVal);
    }

    public static void removeElementAttr(Element element, String attr) {
        element.removeAttribute(attr);
    }

    public static void addElement(Element parent, Element child) {
        parent.appendChild(child);
    }

    public static void addElement(Element parent, String tagName) {
        Document doc = parent.getOwnerDocument();
        Element child = doc.createElement(tagName);
        parent.appendChild(child);
    }

    public static void addElement(Element parent, String tagName, String text) {
        Document doc = parent.getOwnerDocument();
        Element child = doc.createElement(tagName);
        setElementValue(child, text);
        parent.appendChild(child);
    }

    public static void removeElement(Element parent, String tagName) {
        NodeList nl = parent.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node nd = nl.item(i);
            if (nd.getNodeName().equals(tagName)) {
                parent.removeChild(nd);
            }
        }
    }
}
