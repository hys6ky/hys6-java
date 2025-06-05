package hyren.serv6.stream.agent.producer.string.file.dirString;

import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.stream.agent.producer.commons.FileDataValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ReadLineOperatorString {

    private static final Logger logger = LogManager.getLogger();

    public void readLine(String filePath) {
        RandomAccessFile readFile;
        String charset = null;
        try {
            readFile = new RandomAccessFile(new File(filePath), "r");
            StringBuilder lineBuffer = new StringBuilder();
            while (true) {
                String line = readFile.readLine();
                if (line != null) {
                    if (isNewLine(line)) {
                        if (lineBuffer.length() < 1) {
                            lineBuffer.append(line);
                        } else {
                            String message = new String(lineBuffer.toString().getBytes(DataBaseCode.ISO_8859_1.getValue()), charset);
                            logger.info(message);
                        }
                    } else {
                        lineBuffer.append("\n").append(line);
                    }
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            logger.error(e);
            throw new BusinessException(e.getMessage());
        }
    }

    private boolean isNewLine(String line) {
        FileDataValidator fileDataValidator = null;
        String filedataValidatorImplClassName = "";
        if (!filedataValidatorImplClassName.isEmpty()) {
            Class<?> clazz;
            try {
                clazz = Class.forName(filedataValidatorImplClassName);
                fileDataValidator = (FileDataValidator) clazz.newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        if (fileDataValidator.isNewLine(line)) {
            return fileDataValidator.isNewLine(line);
        } else {
            return true;
        }
    }
}
