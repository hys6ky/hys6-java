package hyren.serv6.commons.ocr;

import hyren.serv6.commons.utils.fileutil.read.ReadFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import javax.imageio.ImageIO;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

@Slf4j
public abstract class XmlRpcServer {

    XmlRpcClientConfigImpl config = null;

    XmlRpcClient client = null;

    public XmlRpcServer(String url) {
        try {
            config = new XmlRpcClientConfigImpl();
            config.setServerURL(new URL(url));
            client = new XmlRpcClient();
            client.setConfig(config);
        } catch (MalformedURLException e) {
            log.info("OCR PRC服务连接失败......");
            e.printStackTrace();
        }
    }

    public String extractText(String filePath) {
        String question = null;
        try {
            File file = FileUtils.getFile(filePath);
            Image image = ImageIO.read(file);
            if (image == null) {
                return null;
            }
            question = byteToStr(ReadFileUtil.readBytesForPicture(filePath), FilenameUtils.getExtension(file.getName()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return question;
    }

    public String byteToStr(byte[] bytes, String suffix) {
        String question = null;
        try {
            Object[] params = new Object[] { bytes, suffix };
            question = (String) client.execute("ocr_main", params);
        } catch (XmlRpcException e) {
            e.printStackTrace();
        }
        return question;
    }
}
