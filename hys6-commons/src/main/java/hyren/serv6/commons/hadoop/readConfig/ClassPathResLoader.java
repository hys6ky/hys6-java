package hyren.serv6.commons.hadoop.readConfig;

import hyren.serv6.base.exception.AppSystemException;
import java.io.File;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.jar.JarFile;

public class ClassPathResLoader {

    private static final Method addURL = initAddMethod();

    private static Instrumentation inst = null;

    public static void agentmain(final String a, final Instrumentation inst) {
        ClassPathResLoader.inst = inst;
    }

    private static Method initAddMethod() {
        try {
            Method add = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            add.setAccessible(true);
            return add;
        } catch (Exception e) {
            throw new AppSystemException(e);
        }
    }

    public static void loadResourceDir(String dirPath) {
        File file = new File(dirPath);
        loopDirs(file);
    }

    private static void loopDirs(File file) {
        if (file.isDirectory()) {
            addURL(file);
            File[] tmps = file.listFiles();
            if (null == tmps) {
                return;
            }
            for (File tmp : tmps) {
                loopDirs(tmp);
            }
        }
    }

    private static void addURL(File file) {
        ClassLoader classloader = ClassLoader.getSystemClassLoader();
        try {
            if (!(classloader instanceof URLClassLoader)) {
                inst.appendToSystemClassLoaderSearch(new JarFile(file));
                return;
            }
            addURL.invoke(classloader, file.toURI().toURL());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
