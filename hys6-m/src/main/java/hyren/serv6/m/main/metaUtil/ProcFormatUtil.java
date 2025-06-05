package hyren.serv6.m.main.metaUtil;

public class ProcFormatUtil {

    public static String formatSql(String funcSql) {
        return funcSql.replaceAll("\\r?\\n", " ");
    }
}
