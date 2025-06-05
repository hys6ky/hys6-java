package hyren.serv6.commons.hadoop.i;

import java.util.List;

public interface IOcrExtact {

    void OCR2AvroAndSolr(List<String> fcsPathList);

    boolean isOcrFile(String fileName);
}
