package hyren.serv6.commons.utils.stream;

public class AvroUtil {

    private static final String schemaHeader = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"avro\",\"fields\":[";

    private static final String spritName = "{\"name\":\"";

    private static final String spritTypeString = "\",\"type\":[\"string\",\"null\"]},";

    private static final String spritTypeStringOrc = "\",\"type\":\"string\"},";

    private static final String spritTypeByte = "\",\"type\":[\"bytes\",\"null\"]},";

    private static final String spritTypeByteorc = "\",\"type\":\"bytes\"},";

    private static final String spritTypeLast = "]}";

    public static String getSchemaheader() {
        return schemaHeader;
    }

    public static String getSpritname() {
        return spritName;
    }

    public static String getSprittypestring() {
        return spritTypeString;
    }

    public static String getSprittypebyte() {
        return spritTypeByte;
    }

    public static String getSprittypelast() {
        return spritTypeLast;
    }

    public static String getSprittypestringorc() {
        return spritTypeStringOrc;
    }

    public static String getSprittypebyteorc() {
        return spritTypeByteorc;
    }
}
