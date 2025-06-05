package hyren.serv6.m.util;

import org.springframework.util.ClassUtils;
import org.springframework.util.ResourceUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public abstract class ResourceUtil {

    public static URL getResourceURL(String p_location) {
        URL url = null;
        try {
            ClassLoader cl = ClassUtils.getDefaultClassLoader();
            url = cl.getResource(p_location);
        } catch (Exception e) {
            return null;
        }
        return url;
    }

    public static InputStream getResourceAsStream(String p_location) {
        ClassLoader cl = ClassUtils.getDefaultClassLoader();
        InputStream in = cl.getResourceAsStream(p_location);
        return in;
    }

    public static File getResourceFile(String p_location) throws IOException {
        File file = null;
        URL url = getResourceURL(p_location);
        try {
            if (url == null) {
                file = ResourceUtils.getFile(p_location);
            } else {
                file = ResourceUtils.getFile(url);
            }
            return file;
        } catch (Exception e) {
            throw new IOException("loading file failed. from " + p_location);
        }
    }
}
