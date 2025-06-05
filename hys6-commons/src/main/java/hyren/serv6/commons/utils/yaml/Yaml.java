package hyren.serv6.commons.utils.yaml;

import hyren.daos.base.exception.internal.FrameworkRuntimeException;
import java.io.*;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.springframework.core.io.Resource;

public class Yaml {

    private static String indentAmount = "  ";

    private static PrintWriter out;

    private static String encoding = "UTF-8";

    public static void dump(Object obj, String file) throws FileNotFoundException {
        dump(obj, new File(file));
    }

    public static void dump(Object obj, File file) throws FileNotFoundException {
        try {
            out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), encoding));
            writeObject(obj, "", obj.getClass());
        } catch (UnsupportedEncodingException e) {
            throw new FrameworkRuntimeException("Unsupported encoding " + encoding);
        } finally {
            if (out != null) {
                out.flush();
                out.close();
            }
        }
    }

    public static void copyYml(Resource application, File file) throws IOException {
        Files.copy(application.getInputStream(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    public static void copyXml(Resource log4j2, String log_dir, String log_file_name, File file) throws FileNotFoundException, IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(log4j2.getFile()), StandardCharsets.UTF_8));
        StringBuilder content = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.contains("--log_dir--")) {
                line = line.replaceAll("--log_dir--", log_dir);
            }
            if (line.contains("--log_file_name--")) {
                line = line.replaceAll("--log_file_name--", log_file_name);
            }
            content.append(line).append("\n");
        }
        reader.close();
        Files.write(file.toPath(), content.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void writeObject(Object obj, String index, Class classname) {
        if (obj == null)
            out.println("~");
        if (Map.class.isAssignableFrom(classname)) {
            writeMap((Map) obj, index);
        } else if (classname.isArray()) {
            writeArry(obj, index);
        } else if (Collection.class.isAssignableFrom(classname)) {
            writeCollection((Collection) obj, index);
        } else if (classname.isEnum()) {
        } else if (isSimpleType(classname)) {
            writeSimpleValue(obj);
        } else if (classname instanceof Object) {
            Map<String, Object> objectMap = writeBean(obj, index);
            writeMap(objectMap, index);
        } else {
            writeMap((Map) obj, index);
        }
    }

    private static void writeMap(Map map, String indent) {
        if (map.size() == 0)
            return;
        else {
            out.println("");
            for (Object key : map.keySet()) {
                Object value = map.get(key);
                out.print(indent + key + ": ");
                writeObject(value, indent(indent), value.getClass());
            }
        }
    }

    private static void writeCollection(Collection col, String indent) {
        if (col.size() == 0)
            return;
        else {
            out.println("");
            for (Object o : col) {
                out.print(indent + "-");
                writeObject(o, indent(indent), o.getClass());
            }
        }
    }

    private static void writeArry(Object obj, String indent) {
        Object[] objArry = (Object[]) obj;
        if (objArry.length == 0)
            return;
        else {
            out.println("");
            for (Object val : objArry) {
                out.print(indent + "-");
                writeObject(val, indent(indent), val.getClass());
            }
        }
    }

    private static Map<String, Object> writeBean(Object obj, String indent) {
        Map<String, Object> map = new HashMap<>();
        if (obj == null)
            return map;
        else {
            Class clazz = obj.getClass();
            Field[] fields = clazz.getDeclaredFields();
            try {
                for (Field field : fields) {
                    field.setAccessible(true);
                    String key = field.getName();
                    Object value = field.get(obj);
                    if (value != null && !isNumber(value, value.getClass()))
                        map.put(key, value);
                }
                return map;
            } catch (Exception e) {
                throw new FrameworkRuntimeException("Entity transition exception " + e);
            }
        }
    }

    private static String indent(String s) {
        return indentAmount + s;
    }

    private static boolean isSimpleType(Class c) {
        return c.isPrimitive() || c == String.class || c == Integer.class || c == Long.class || c == Short.class || c == Double.class || c == Float.class || c == Boolean.class || c == Character.class;
    }

    private static boolean isNumber(Object o, Class c) {
        return ((c == Integer.class || c == Long.class || c == Short.class || c == Double.class || c == Float.class || c == Boolean.class) && new BigDecimal(o.toString()).compareTo(BigDecimal.ZERO) == 0);
    }

    private static void writeSimpleValue(Object value) {
        if (value == null)
            out.println("~");
        else if (value instanceof String || value instanceof Character)
            out.println(quote(value.toString()));
        else
            out.println(value);
    }

    private static String quote(String value) {
        return "\"" + value + "\"";
    }
}
