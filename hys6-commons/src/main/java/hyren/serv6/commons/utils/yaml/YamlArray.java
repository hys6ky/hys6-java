package hyren.serv6.commons.utils.yaml;

import java.util.LinkedList;
import java.util.Map;

public class YamlArray extends LinkedList {

    public YamlMap getMap(int index) {
        YamlMap yamlMap = new YamlMap();
        yamlMap.putAll((Map) this.get(index));
        return yamlMap;
    }
}
