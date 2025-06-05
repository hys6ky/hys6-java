package hyren.serv6.commons.hadoop.i;

import java.util.Map;

public interface IAvroRecord {

    byte[] getFileBytesFromAvro(Long fileAvroBlock, String fileAvroPath);

    Map<String, Object> getFileContents(Long fileAvroBlock, String fileAvroPath);

    String getOcrText(String file_avro_path, String name, String uuid);
}
