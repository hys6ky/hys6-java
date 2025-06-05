package hyren.serv6.commons.utils.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class BusinessProcessUtil {

    @SuppressWarnings("rawtypes")
    public static ConsumerBusinessProcess buspro(String bus_pro_cla, Map<String, Object> jsonStore) {
        ConsumerBusinessProcess buspro = null;
        if (!StringUtils.isBlank(bus_pro_cla)) {
            try {
                Class<?> clazz = Class.forName(bus_pro_cla);
                Constructor<?> ct = clazz.getConstructor(Map.class);
                buspro = (ConsumerBusinessProcess) ct.newInstance(jsonStore);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | SecurityException | IllegalArgumentException | InvocationTargetException e) {
                log.info("加载业务处理类失败！！！", e);
                System.exit(-1);
            }
        }
        return buspro;
    }

    public RealizeBusinessProcess buspro(String bus_pro_cla) {
        RealizeBusinessProcess buspro = null;
        if (!StringUtils.isBlank(bus_pro_cla)) {
            try {
                CusClassLoader classLoader = new CusClassLoader();
                Class<?> clazz = classLoader.getURLClassLoader().loadClass(bus_pro_cla);
                buspro = (RealizeBusinessProcess) clazz.newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SecurityException | IllegalArgumentException | MalformedURLException e) {
                log.info("加载业务处理类失败！！！", e);
            }
        }
        return buspro;
    }

    public ProcessorSupplier<?, ?> buspro(String bus_pro_cla, Map<String, Object> jsonStore, Map<String, Object> jsonParm, CountDownLatch threadSignal, String stateStoreName) {
        ProcessorSupplier<?, ?> buspro = null;
        if (!StringUtils.isBlank(bus_pro_cla)) {
            try {
                CusClassLoader classLoader = new CusClassLoader();
                Class<?> clazz = classLoader.getURLClassLoader().loadClass(bus_pro_cla);
                Constructor<?> ct = clazz.getConstructor(Map.class, String.class, CountDownLatch.class, String.class, Long.class);
                buspro = (ProcessorSupplier<?, ?>) ct.newInstance(jsonStore, jsonParm.get("bootstrap_servers").toString(), threadSignal, stateStoreName, Long.valueOf(jsonParm.get("punctuate.time").toString()));
            } catch (ClassNotFoundException | MalformedURLException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                log.info("加载业务处理类失败！！！", e);
            }
        }
        return buspro;
    }
}
