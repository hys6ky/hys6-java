package hyren.serv6.base.utils.regular;

import fd.ng.core.annotation.DocClass;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@DocClass(desc = "", author = "dhw", createdate = "2021/4/15 9:20")
public class RegexConstant {

    public static final String IP_VERIFICATION = "(?=(\\b|\\D))(((\\d{1,2})|(1\\d{1,2})|(2[0-4]\\d)|(25[0-5]))\\.)" + "{3}((\\d{1,2})|(1\\d{1,2})|(2[0-4]\\d)|(25[0-5]))(?=(\\b|\\D))";

    public static final String PORT_VERIFICATION = "^(1(02[4-9]|0[3-9][0-9]|[1-9][0-9]{2})" + "|[2-9][0-9]{3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$";

    public static final String IS_CHINESE = "[\\u4e00-\\u9fa5]";

    public static final String NO_ALPHABET = "[^A-Za-z]+";

    public static final String IS_ALPHABET = "^[A-Za-z]+$";

    public static final String NO_NUMBER = "[^0-9]+";

    public static final String EMAIL_VERIFICATION = "^\\w+([-+.]\\w+)*@\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*$";

    public static final String POSITIVE_NUMBER = "^[0-9]*[1-9][0-9]*$";

    public static final String FRONT_LETTER = "^[a-zA-Z]\\w*$";

    public static final String NUMBER_ENGLISH_UNDERSCORE = "^[0-9a-zA-Z_]+$";

    public static final String TIME_FORMAT = "^(?:[01]\\d|2[0-3])(?::[0-5]\\d){2}$";

    public static boolean matcher(String regex, String input) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);
        return matcher.matches();
    }
}
