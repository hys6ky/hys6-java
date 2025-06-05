package hyren.serv6.m.config;

import hyren.daos.base.exception.SystemRuntimeException;
import hyren.serv6.base.codes.CodesItem;
import hyren.serv6.m.contants.MetaObjTypeEnum;
import hyren.serv6.m.contants.MetadataSourceEnum;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.*;
import org.springframework.util.CollectionUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

@Slf4j
@Configuration
public class EnumRegisterConfig {

    static {
        CodesItem.mapCat.put("MetadataSourceEnum", MetadataSourceEnum.class);
        CodesItem.mapCat.put("MetaObjTypeEnum", MetaObjTypeEnum.class);
    }

    private static final String CLASS_SUFFIX = ".class";

    private static final String CLASS_FILE_PREFIX = File.separator + "classes" + File.separator;

    private static final String PACKAGE_SEPARATOR = ".";

    @Resource
    private RegisterEnumProperties registerEnumProperties;

    @PostConstruct
    public void registerEnum() {
        if (CollectionUtils.isEmpty(registerEnumProperties.getPackages())) {
            log.debug("没有需要注册的代码项");
            return;
        }
        for (String registerPackage : registerEnumProperties.getPackages()) {
            List<String> className = getClazzName(registerPackage, true);
            log.info("注册代码项：{},到base服务", className);
            for (String pckClass : className) {
                Class<?> clazz;
                try {
                    clazz = Class.forName(pckClass);
                    if (clazz.isEnum()) {
                        sendMsg(clazz);
                    }
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void sendMsg(Class<?> clazz) {
        try {
            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
            MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
            body.add("category", clazz.getSimpleName());
            body.add("obj", getSerialized(clazz));
            HttpEntity httpEntity = new HttpEntity(body, httpHeaders);
            ResponseEntity<String> response = new RestTemplate().exchange(registerEnumProperties.getUrl(), HttpMethod.POST, httpEntity, String.class);
            log.info("代码项注册结果：{}", response);
        } catch (Exception e) {
            log.warn("代码项注册失败，原因：{}", e.getMessage());
        }
    }

    public static String getSerialized(Class<?> clazz) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(clazz);
            String obj = Base64.getEncoder().encodeToString(baos.toByteArray());
            return obj;
        } catch (Exception e) {
            throw new SystemRuntimeException(e);
        }
    }

    public static List<String> getClazzName(String packageName, boolean showChildPackageFlag) {
        List<String> result = new ArrayList<>();
        String suffixPath = packageName.replaceAll("\\.", "/");
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {
            Enumeration<URL> urls = loader.getResources(suffixPath);
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url != null) {
                    String protocol = url.getProtocol();
                    if ("file".equals(protocol)) {
                        String path = url.getPath();
                        System.out.println(path);
                        result.addAll(getAllClassNameByFile(new File(path), showChildPackageFlag));
                    } else if ("jar".equals(protocol)) {
                        JarFile jarFile = null;
                        try {
                            jarFile = ((JarURLConnection) url.openConnection()).getJarFile();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        if (jarFile != null) {
                            result.addAll(getAllClassNameByJar(jarFile, packageName, showChildPackageFlag));
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private static List<String> getAllClassNameByFile(File file, boolean flag) {
        List<String> result = new ArrayList<>();
        if (!file.exists()) {
            return result;
        }
        if (file.isFile()) {
            String path = file.getPath();
            if (path.endsWith(CLASS_SUFFIX)) {
                path = path.replace(CLASS_SUFFIX, "");
                String clazzName = path.substring(path.indexOf(CLASS_FILE_PREFIX) + CLASS_FILE_PREFIX.length()).replace(File.separator, PACKAGE_SEPARATOR);
                if (-1 == clazzName.indexOf("$")) {
                    result.add(clazzName);
                }
            }
            return result;
        } else {
            File[] listFiles = file.listFiles();
            if (listFiles != null && listFiles.length > 0) {
                for (File f : listFiles) {
                    if (flag) {
                        result.addAll(getAllClassNameByFile(f, flag));
                    } else {
                        if (f.isFile()) {
                            String path = f.getPath();
                            if (path.endsWith(CLASS_SUFFIX)) {
                                path = path.replace(CLASS_SUFFIX, "");
                                String clazzName = path.substring(path.indexOf(CLASS_FILE_PREFIX) + CLASS_FILE_PREFIX.length()).replace(File.separator, PACKAGE_SEPARATOR);
                                if (-1 == clazzName.indexOf("$")) {
                                    result.add(clazzName);
                                }
                            }
                        }
                    }
                }
            }
            return result;
        }
    }

    private static List<String> getAllClassNameByJar(JarFile jarFile, String packageName, boolean flag) {
        List<String> result = new ArrayList<>();
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
            JarEntry jarEntry = entries.nextElement();
            String name = jarEntry.getName();
            if (name.endsWith(CLASS_SUFFIX)) {
                name = name.replace(CLASS_SUFFIX, "").replace("/", ".");
                if (flag) {
                    if (name.startsWith(packageName) && -1 == name.indexOf("$")) {
                        result.add(name);
                    }
                } else {
                    if (packageName.equals(name.substring(0, name.lastIndexOf("."))) && -1 == name.indexOf("$")) {
                        result.add(name);
                    }
                }
            }
        }
        return result;
    }

    @Data
    @Configuration
    @ConfigurationProperties(prefix = "base.register.enum")
    class RegisterEnumProperties {

        private String url;

        private List<String> packages;
    }
}
