package hyren.serv6.commons.hadoop.fileparser;

import java.io.IOException;
import java.util.List;

public interface FileParserInterface {

    String parserFile();

    void dealLine(List<String> lineList) throws IOException;

    void stopStream() throws IOException;
}
