package hyren.serv6.commons.utils.yaml;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.YamlMapFactoryBean;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class YamlFactory {

    public static YamlMap yaml2Map(String yamlSource) {
        try {
            YamlMapFactoryBean yaml = new YamlMapFactoryBean();
            yaml.setResources(new ClassPathResource(yamlSource));
            YamlMap y = new YamlMap();
            y.putAll(yaml.getObject());
            return y;
        } catch (Exception e) {
            log.error("Cannot read yaml", e);
            return null;
        }
    }

    public static Properties yaml2Properties(String yamlSource) {
        try {
            YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
            yaml.setResources(new ClassPathResource(yamlSource));
            return yaml.getObject();
        } catch (Exception e) {
            log.error("Cannot read yaml", e);
            return null;
        }
    }

    public static void main(String[] args) {
        Map<String, Map> mapdata = new HashMap<>();
        YamlMap map = YamlFactory.yaml2Map(ConfFileLoader.getConfFile("lengthMapping"));
        YamlArray arrays = map.getArray("defaultlengthmapping");
        for (int i = 0; i < arrays.size(); i++) {
            YamlMap trans = arrays.getMap(i);
            mapdata.put(trans.getString("NAME"), trans);
        }
        Map db2V1 = mapdata.get("DB2_V1");
        System.out.println(db2V1.get("DOUBLE"));
        YamlMap control = YamlFactory.yaml2Map(ConfFileLoader.getConfFile("control"));
        YamlMap hazelcast = control.getMap("hazelcast");
        YamlMap notify = control.getMap("notify");
        System.out.println(hazelcast.getString("localAddress"));
        System.out.println(hazelcast.getInt("autoIncrementPort"));
        System.out.println(hazelcast.getInt("portCount"));
    }
}
