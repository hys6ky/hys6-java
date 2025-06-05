package hyren.serv6.commons.utils.yaml;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class YamlMap extends LinkedHashMap {

    public YamlMap getMap(String key) {
        this.putAll((Map) this.get(key));
        return this;
    }

    public YamlArray getArray(String key) {
        YamlArray arry = new YamlArray();
        arry.addAll((Collection) this.get(key));
        return arry;
    }

    public String getString(String key) {
        return toString(this.get(key));
    }

    public boolean exist(final String key) {
        return this.containsKey(key);
    }

    public String getString(String key, final String defaultValue) {
        String orgnValue = toString(this.get(key));
        return orgnValue == null ? defaultValue : this.fixString(toString(this.get(key)));
    }

    public int getInt(final String key) {
        return Integer.parseInt(toString(this.get(key)));
    }

    public int getInt(final String key, final int defaultValue) {
        String orgnValue = toString(this.get(key));
        return orgnValue == null ? defaultValue : Integer.parseInt(orgnValue);
    }

    public long getLong(final String key) {
        return Long.parseLong(toString(this.get(key)));
    }

    public long getLong(final String key, final long defaultValue) {
        String orgnValue = toString(this.get(key));
        return orgnValue == null ? defaultValue : Long.parseLong(orgnValue);
    }

    public BigDecimal getDecimal(final String key) {
        return new BigDecimal(toString(this.get(key)));
    }

    public BigDecimal getDecimal(final String key, final BigDecimal defaultValue) {
        String orgnValue = toString(this.get(key));
        return orgnValue == null ? defaultValue : new BigDecimal(orgnValue);
    }

    public boolean getBool(final String key) {
        return Boolean.parseBoolean(toString(this.get(key)));
    }

    public boolean getBool(final String key, final boolean defaultValue) {
        String orgnValue = toString(this.get(key));
        return orgnValue == null ? defaultValue : Boolean.parseBoolean(orgnValue);
    }

    protected String toString(final Object str) {
        return String.valueOf(str);
    }

    protected String fixString(final String str) {
        int len = str.length();
        if (str.charAt(0) == '"' && str.charAt(len - 1) == '"') {
            String ret = str.substring(1, len - 1);
            return ret == null ? "" : ret;
        } else {
            return str;
        }
    }
}
