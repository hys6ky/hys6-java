package hyren.serv6.hadoop.commons.imp;

import hyren.serv6.commons.hadoop.i.IOcrExtact;
import hyren.serv6.hadoop.commons.ocr.OcrExtractText;
import java.util.List;

public class OcrExtactImp implements IOcrExtact {

    @Override
    public void OCR2AvroAndSolr(List<String> fcsPathList) {
        for (String fcsPath : fcsPathList) {
            OcrExtractText oet = new OcrExtractText(fcsPath);
            oet.OCR2AvroAndSolr();
        }
    }

    @Override
    public boolean isOcrFile(String s) {
        return OcrExtractText.isOcrFile(s);
    }
}
