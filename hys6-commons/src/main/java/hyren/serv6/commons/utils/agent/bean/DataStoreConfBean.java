package hyren.serv6.commons.utils.agent.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.FileNameUtils;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@DocClass(desc = "", author = "zxz", createdate = "2019/11/29 21:15")
public class DataStoreConfBean implements Serializable {

    @DocBean(name = "dsl_id", value = "", dataType = Long.class)
    private Long dsl_id;

    @DocBean(name = "dsl_name", value = "", dataType = String.class)
    private String dsl_name;

    @DocBean(name = "store_type", value = "", dataType = String.class)
    private String store_type;

    @DocBean(name = "data_store_connect_attr", value = "", dataType = Map.class)
    private Map<String, String> data_store_connect_attr;

    @DocBean(name = "additInfoField", value = "", dataType = Map.class)
    private Map<String, Map<String, Integer>> additInfoFieldMap;

    @DocBean(name = "is_hadoopclient", value = "", dataType = String.class)
    private String is_hadoopclient;

    @DocBean(name = "data_store_layer_file", value = "", dataType = Map.class)
    private Map<String, String> data_store_layer_file;

    public String getDsl_name() {
        return dsl_name;
    }

    public void setDsl_name(String dsl_name) {
        this.dsl_name = dsl_name;
    }

    public String getStore_type() {
        return store_type;
    }

    public void setStore_type(String store_type) {
        this.store_type = store_type;
    }

    public Map<String, String> getData_store_connect_attr() {
        if (data_store_connect_attr != null && !data_store_connect_attr.isEmpty()) {
            String external_root_path = data_store_connect_attr.get(StorageTypeKey.external_root_path);
            if (external_root_path != null) {
                if (external_root_path.endsWith("/") || external_root_path.endsWith("\\")) {
                    data_store_connect_attr.put(StorageTypeKey.external_root_path, FileNameUtils.normalize(external_root_path, true));
                } else {
                    external_root_path = external_root_path + File.separator;
                    data_store_connect_attr.put(StorageTypeKey.external_root_path, FileNameUtils.normalize(external_root_path, true));
                }
            }
        }
        String dsla_dir = FileNameUtils.normalize(Constant.STORECONFIGPATH + dsl_name, true);
        data_store_layer_file.forEach((k, v) -> {
            data_store_connect_attr.put(k, dsla_dir + File.separator + k);
        });
        return data_store_connect_attr;
    }

    public void setData_store_connect_attr(Map<String, String> data_store_connect_attr) {
        this.data_store_connect_attr = data_store_connect_attr;
    }

    public String getIs_hadoopclient() {
        return is_hadoopclient;
    }

    public void setIs_hadoopclient(String is_hadoopclient) {
        this.is_hadoopclient = is_hadoopclient;
    }

    public Map<String, String> getData_store_layer_file() {
        return data_store_layer_file;
    }

    public void setData_store_layer_file(Map<String, String> data_store_layer_file) {
        this.data_store_layer_file = data_store_layer_file;
    }

    public Map<String, Map<String, Integer>> getAdditInfoFieldMap() {
        return additInfoFieldMap;
    }

    public void setAdditInfoFieldMap(Map<String, Map<String, Integer>> additInfoFieldMap) {
        this.additInfoFieldMap = additInfoFieldMap;
    }

    public Map<String, Map<Integer, String>> getSortAdditInfoFieldMap() {
        Map<String, Map<Integer, String>> sortAdditInfoFieldMap = new HashMap<>();
        for (String key : additInfoFieldMap.keySet()) {
            Map<String, Integer> stringIntegerMap = additInfoFieldMap.get(key);
            Map<Integer, String> sortMap = new TreeMap<>();
            for (String key2 : stringIntegerMap.keySet()) {
                sortMap.put(stringIntegerMap.get(key2), key2);
            }
            sortAdditInfoFieldMap.put(key, sortMap);
        }
        return sortAdditInfoFieldMap;
    }

    public static void main(String[] args) {
        DataStoreConfBean dataStoreConfBean = new DataStoreConfBean();
        Map<String, Map<String, Integer>> sortAdditInfoFieldMap = new HashMap<>();
        Map<String, Integer> aaa = new HashMap<>();
        aaa.put("asd", 9);
        aaa.put("ewwe", 7);
        aaa.put("vzxaaa1", 1);
        aaa.put("asdsq", 5);
        aaa.put("qwe", 2);
        aaa.put("aaa", 3);
        sortAdditInfoFieldMap.put("aaa", aaa);
        dataStoreConfBean.setAdditInfoFieldMap(sortAdditInfoFieldMap);
        Map<String, Map<Integer, String>> sortAdditInfoFieldMap1 = dataStoreConfBean.getSortAdditInfoFieldMap();
        for (int key : sortAdditInfoFieldMap1.get("aaa").keySet()) {
            System.out.println(key + "===============" + dataStoreConfBean.getSortAdditInfoFieldMap().get("aaa").get(key));
        }
    }

    public Long getDsl_id() {
        return dsl_id;
    }

    public void setDsl_id(Long dsl_id) {
        this.dsl_id = dsl_id;
    }
}
