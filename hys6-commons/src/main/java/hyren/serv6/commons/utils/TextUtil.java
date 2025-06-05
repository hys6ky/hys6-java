package hyren.serv6.commons.utils;

import com.hankcs.hanlp.summary.TextRankSentence;
import fd.ng.core.annotation.DocClass;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@DocClass(desc = "", author = "BY-HLL", createdate = "2022/3/22 0022 上午 10:47")
public class TextUtil {

    private static final int LIMIT_LENGTH_IN_SOLR_FIELD = 30000;

    public static String replaceMatcher(String str, String matcher, String replaceStr) {
        String dest = "";
        if (str != null) {
            Pattern p = Pattern.compile(matcher);
            Matcher m = p.matcher(str);
            dest = m.replaceAll(replaceStr);
        }
        return dest;
    }

    public static String normalizeText(String text) {
        text = replaceMatcher(text, "(\r|\n){2,}", "<br><br>");
        text = replaceMatcher(text, "\r|\n", "<br>");
        text = replaceMatcher(text, "\t", "");
        if (text.length() > LIMIT_LENGTH_IN_SOLR_FIELD) {
            text = text.substring(0, LIMIT_LENGTH_IN_SOLR_FIELD);
        }
        return text;
    }

    public static String normalizeSummary(String text) {
        text = replaceMatcher(text, "\\s*|\t|\r|\n", "");
        if (text.length() > LIMIT_LENGTH_IN_SOLR_FIELD) {
            text = text.substring(0, LIMIT_LENGTH_IN_SOLR_FIELD);
        }
        return text;
    }

    public static String etractSummary(String text, int summary_volumn) {
        return TextRankSentence.getTopSentenceList(text, summary_volumn).toString();
    }
}
