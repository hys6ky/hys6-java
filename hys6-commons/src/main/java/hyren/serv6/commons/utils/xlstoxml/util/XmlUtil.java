package hyren.serv6.commons.utils.xlstoxml.util;

import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class XmlUtil {

    public static final String INVALID_REGEX = "[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]";

    private final static String DEFAULTDOCUMENTBUILDERFACTORY = "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl";

    private static boolean NAME_SPACE_AWARE = true;

    public final static String UTF_8 = "UTF-8";

    public static final int INDENT_DEFAULT = 2;

    public static Document readXML(String pathOrContent) {
        if (pathOrContent.startsWith("<")) {
            return parseXml(pathOrContent);
        }
        return readXML(new File(pathOrContent));
    }

    public static Document parseXml(String xmlStr) {
        if (StringUtil.isBlank(xmlStr)) {
            throw new IllegalArgumentException("XML content string is empty !");
        }
        xmlStr = cleanInvalid(xmlStr);
        return readXML(new File(xmlStr));
    }

    public static Document readXML(File file) {
        if (!file.exists()) {
            throw new AppSystemException("File [{" + file.getAbsolutePath() + "}] not a exist!");
        }
        try {
            file = file.getCanonicalFile();
        } catch (IOException e) {
        }
        BufferedInputStream in = null;
        try (BufferedInputStream buffer = new BufferedInputStream(new FileInputStream(file))) {
            return readXML(buffer);
        } catch (IOException e) {
            throw new AppSystemException(e);
        }
    }

    public static Document readXML(InputStream inputStream) throws AppSystemException {
        return readXML(new InputSource(inputStream));
    }

    public static Document readXML(InputSource source) {
        final DocumentBuilder builder = createDocumentBuilder();
        try {
            return builder.parse(source);
        } catch (Exception e) {
            throw new AppSystemException(e);
        }
    }

    public static DocumentBuilder createDocumentBuilder() {
        DocumentBuilder builder;
        try {
            builder = createDocumentBuilderFactory().newDocumentBuilder();
        } catch (Exception e) {
            throw new AppSystemException(e);
        }
        return builder;
    }

    public static DocumentBuilderFactory createDocumentBuilderFactory() {
        final DocumentBuilderFactory factory;
        if (StringUtil.isNotEmpty(DEFAULTDOCUMENTBUILDERFACTORY)) {
            factory = DocumentBuilderFactory.newInstance(DEFAULTDOCUMENTBUILDERFACTORY, null);
        } else {
            factory = DocumentBuilderFactory.newInstance();
        }
        factory.setNamespaceAware(NAME_SPACE_AWARE);
        return disableXXE(factory);
    }

    private static DocumentBuilderFactory disableXXE(DocumentBuilderFactory dbf) {
        String feature;
        try {
            feature = "http://apache.org/xml/features/disallow-doctype-decl";
            dbf.setFeature(feature, true);
            feature = "http://xml.org/sax/features/external-general-entities";
            dbf.setFeature(feature, false);
            feature = "http://xml.org/sax/features/external-parameter-entities";
            dbf.setFeature(feature, false);
            feature = "http://apache.org/xml/features/nonvalidating/load-external-dtd";
            dbf.setFeature(feature, false);
            dbf.setXIncludeAware(false);
            dbf.setExpandEntityReferences(false);
        } catch (ParserConfigurationException e) {
        }
        return dbf;
    }

    public static String cleanInvalid(String xmlContent) {
        if (xmlContent == null) {
            return null;
        }
        return xmlContent.replaceAll(INVALID_REGEX, "");
    }

    public static String toStr(Document doc) {
        return toStr(doc, UTF_8, false);
    }

    public static String toStr(Document doc, String charset, boolean isPretty) {
        return toStr(doc, charset, isPretty, false);
    }

    public static String toStr(Document doc, String charset, boolean isPretty, boolean omitXmlDeclaration) {
        StringWriter writer = new StringWriter();
        try {
            write(doc, writer, charset, isPretty ? INDENT_DEFAULT : 0, omitXmlDeclaration);
        } catch (Exception e) {
            throw new AppSystemException(e);
        }
        return writer.toString();
    }

    public static void write(Node node, Writer writer, String charset, int indent, boolean omitXmlDeclaration) {
        transform(new DOMSource(node), new StreamResult(writer), charset, indent, omitXmlDeclaration);
    }

    public static void transform(Source source, Result result, String charset, int indent, boolean omitXmlDeclaration) {
        final TransformerFactory factory = TransformerFactory.newInstance();
        try {
            final Transformer xformer = factory.newTransformer();
            if (indent > 0) {
                xformer.setOutputProperty(OutputKeys.INDENT, "yes");
                xformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", String.valueOf(indent));
            }
            if (StringUtil.isNotBlank(charset)) {
                xformer.setOutputProperty(OutputKeys.ENCODING, charset);
            }
            if (omitXmlDeclaration) {
                xformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            }
            xformer.transform(source, result);
        } catch (Exception e) {
            throw new AppSystemException(e);
        }
    }

    public static void toFile(Document doc, String path, String charset) {
        if (StringUtil.isBlank(charset)) {
            charset = doc.getXmlEncoding();
        }
        if (StringUtil.isBlank(charset)) {
            charset = UTF_8;
        }
        try (BufferedWriter writer = FileUtil.newBufferedWriter(Paths.get(path), Charset.defaultCharset().newEncoder())) {
            write(doc, writer, charset, INDENT_DEFAULT, true);
        } catch (IOException e) {
            throw new AppSystemException(e);
        }
    }

    public static Element getChildElement(Element parent, String childName) {
        NodeList children = parent.getChildNodes();
        int size = children.getLength();
        for (int i = 0; i < size; i++) {
            Node node = children.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element element = (Element) node;
                if (childName.equals(element.getNodeName())) {
                    return element;
                }
            }
        }
        return null;
    }

    public static List getChildElements(Element parent, String childName) {
        NodeList children = parent.getChildNodes();
        List list = new ArrayList();
        int size = children.getLength();
        for (int i = 0; i < size; i++) {
            Node node = children.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element element = (Element) node;
                if (childName.equals(element.getNodeName())) {
                    list.add(element);
                }
            }
        }
        return list;
    }

    public static InputStream getInputStream(URL url) {
        InputStream is = null;
        if (url != null) {
            try {
                is = url.openStream();
            } catch (Exception ex) {
                throw new BusinessException("打开配置文件失败 !");
            }
        }
        return is;
    }

    public static InputStream getInputStream(String fileName) {
        InputStream is = null;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (is == null) {
            try {
                is = classLoader.getResourceAsStream(fileName);
            } catch (Exception e) {
                throw new BusinessException("打开配置文件失败 !" + e.getMessage());
            }
        }
        return is;
    }

    public static String getChildText(Element parent, String childName) {
        Element child = getChildElement(parent, childName);
        if (child == null) {
            return null;
        }
        return getText(child);
    }

    public static String getText(Element node) {
        StringBuffer sb = new StringBuffer();
        NodeList list = node.getChildNodes();
        for (int i = 0; i < list.getLength(); i++) {
            Node child = list.item(i);
            switch(child.getNodeType()) {
                case Node.CDATA_SECTION_NODE:
                case Node.TEXT_NODE:
                    sb.append(child.getNodeValue());
            }
        }
        return sb.toString();
    }

    public static String encode(Object string) {
        if (string == null) {
            return "";
        }
        char[] chars = string.toString().toCharArray();
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < chars.length; i++) {
            switch(chars[i]) {
                case '&':
                    out.append("&amp;");
                    break;
                case '<':
                    out.append("&lt;");
                    break;
                case '>':
                    out.append("&gt;");
                    break;
                case '\"':
                    out.append("&quot;");
                    break;
                default:
                    out.append(chars[i]);
            }
        }
        return out.toString();
    }
}
