package hyren.serv6.commons.dataclean;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.xlstoxml.util.XmlUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CleanFactory {

    private static final Map<String, String> mpTypeClass = new HashMap<>();

    private static final CleanFactory CF = new CleanFactory();

    private CleanFactory() {
        mpTypeClass.put("clean_database", "hyren.serv6.commons.dataclean.DataClean_Biz");
        mpTypeClass.put("clean_db_file", "hyren.serv6.commons.dataclean.DataClean_Biz");
    }

    public static CleanFactory getInstance() {
        return CF;
    }

    public DataCleanInterface getObjectClean(String type) throws Exception {
        try {
            String classZ = mpTypeClass.get(type);
            if (!StringUtil.isEmpty(classZ)) {
                return (DataCleanInterface) Class.forName(classZ).newInstance();
            } else {
                throw new AppSystemException("配置中没有找到：" + type + "的配置信息");
            }
        } catch (Exception e) {
            throw new AppSystemException(String.format("获取配置配置类失败:%s", e));
        }
    }

    private static void loadCfgInfo(InputStream is, File file) throws Exception {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            DocumentBuilder db;
            try {
                db = dbf.newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                throw new AppSystemException("创建文档管理器失败", e);
            }
            Document doc;
            if (null != file) {
                doc = db.parse(file);
            } else {
                doc = db.parse(is);
            }
            Element root = (Element) doc.getElementsByTagName("beans").item(0);
            List<?> beanList = XmlUtil.getChildElements(root, "bean");
            for (Object b : beanList) {
                Element bean = (Element) b;
                String typeid = bean.getAttribute("id");
                String InfoClass = bean.getAttribute("class");
                mpTypeClass.put(typeid, InfoClass);
            }
        } catch (Exception ex) {
            throw new AppSystemException("加载FactoryConf的配置信息异常！ ", ex);
        }
    }
}
