package hyren.serv6.commons.dataclean;

import hyren.serv6.base.entity.ColumnSplit;
import java.util.List;
import java.util.Map;

public interface DataCleanInterface {

    public String replace(Map<String, Map<String, String>> deleSpecialSpace, String columnData, String columnname);

    public String filling(Map<String, String> strFilling, String columnData, String columnname);

    public String dateing(Map<String, String> dateing, String columnData, String columnname);

    public String split(Map<String, Map<String, ColumnSplit>> spliting, String columnData, String columnname, String type, String fileType, List<Object> list, String database_code, String database_separatorr);

    public String merge(Map<String, String> mergeing, String[] arrColString, String[] columns, List<Object> lineData, String filetype, String database_code, String database_separatorr);

    public String codeTrans(Map<String, String> coding, String columnData, String columnname);

    public String trim(Map<String, String> Triming, String columnData, String columnname);
}
