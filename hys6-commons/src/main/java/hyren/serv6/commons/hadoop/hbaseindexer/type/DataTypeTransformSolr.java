package hyren.serv6.commons.hadoop.hbaseindexer.type;

import fd.ng.core.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DataTypeTransformSolr {

    private static final Map<String, String> map_solr = new HashMap<>();

    private static final Map<String, String> map_solr_customize_type = new HashMap<>();

    private static final String LKH = "(";

    static {
        map_solr.put("STRING", "STRING");
        map_solr.put("VARCHAR", "STRING");
        map_solr.put("INT", "INT");
        map_solr.put("LONG", "LONG");
        map_solr.put("DOUBLE", "DOUBLE");
        map_solr.put("FLOAT", "FLOAT");
        map_solr.put("DECIMAL", "STRING");
        map_solr.put("BOOLEAN", "BOOLEAN");
        map_solr.put("DATE", "STRING");
        map_solr.put("TIMESTAMP", "STRING");
        initializeSolrCustomizeType();
    }

    private static void initializeSolrCustomizeType() {
        map_solr_customize_type.put("STRING", "customization.hyren.type.Hstring");
        map_solr_customize_type.put("INT", "customization.hyren.type.Hint");
        map_solr_customize_type.put("BIGDECIMAL", "customization.hyren.type.Hbigdecimal");
        map_solr_customize_type.put("BOOLEAN", "customization.hyren.type.Hboolean");
        map_solr_customize_type.put("DOUBLE", "customization.hyren.type.Hdouble");
        map_solr_customize_type.put("FLOAT", "customization.hyren.type.Hfloat");
        map_solr_customize_type.put("LONG", "customization.hyren.type.Hlong");
        map_solr_customize_type.put("SHORT", "customization.hyren.type.Hshort");
    }

    public static String transform(String type) {
        type = type.trim().toUpperCase();
        String key = type.contains(LKH) ? type.substring(0, type.indexOf(LKH)) : type;
        String transformedType = map_solr.get(key);
        if (StringUtil.isBlank(transformedType)) {
            log.info("no configuration type: " + key + ", using default type STRING");
            transformedType = "STRING";
        }
        return transformedType;
    }

    public static String transformToRealTypeInSolr(String transformedType) {
        String customizeTypeInSolr = map_solr_customize_type.get(transformedType);
        if (StringUtil.isBlank(customizeTypeInSolr)) {
            log.info("Can not transform type " + transformedType + " to customize type in solr, using default customize type " + map_solr_customize_type.get("STRING"));
            return map_solr_customize_type.get("STRING");
        }
        return customizeTypeInSolr;
    }

    public static List<String> tansform(List<String> types) {
        List<String> transformedTypeList = new ArrayList<>();
        for (String string : types) {
            transformedTypeList.add(transform(string));
        }
        return transformedTypeList;
    }

    public static void main(String[] args) {
        System.out.println(transform("Integer(20,3)"));
    }
}
