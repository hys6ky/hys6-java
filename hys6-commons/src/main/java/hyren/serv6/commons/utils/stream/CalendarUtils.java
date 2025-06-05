package hyren.serv6.commons.utils.stream;

import org.springframework.stereotype.Component;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Component
public class CalendarUtils {

    public static long convertDate2UnixTime(String date) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.parse(date).getTime();
    }

    public static String convertTimeMill2Date(long timeMill) {
        long day = timeMill / (3600 * 24);
        long hour = (timeMill - 3600 * 24 * day) / (60 * 60);
        long min = (timeMill - 3600 * 24 * day - 3600 * hour) / 60;
        long sec = timeMill - 3600 * 24 * day - 3600 * hour - 60 * min;
        return day + "Day" + hour + "Hour" + min + "min" + sec + "sec";
    }

    public static String convertUnixTime(long unixtime) {
        String formatter = "yyyy-MM-dd HH:mm:ss";
        return convertUnixTime(unixtime, formatter);
    }

    public static String convertUnixTime(long unixtime, String formatter) {
        SimpleDateFormat df = new SimpleDateFormat(formatter);
        return df.format(new Date(unixtime));
    }

    public static String convertUnixTime2Date(long unixtime) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date(unixtime));
    }

    public static String getDate() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date());
    }

    public static String getCustomDate(String formatter) {
        SimpleDateFormat df = new SimpleDateFormat(formatter);
        return df.format(new Date());
    }
}
