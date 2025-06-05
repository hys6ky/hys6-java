package hyren.serv6.stream.agent.producer.avro.file.file;

import hyren.serv6.stream.agent.producer.commons.FileDataValidator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileDataValidatorImpl implements FileDataValidator {

    public static final Pattern patter = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})[\\S\\s]*");

    @Override
    public boolean isNewLine(String lineText) {
        Matcher matcher = patter.matcher(lineText);
        return matcher.matches();
    }

    @Override
    public boolean isSkipLine(String lineText) {
        return false;
    }
}
