package hyren.serv6.commons.utils.stream;

import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.kafka.common.errors.SerializationException;
import java.nio.ByteBuffer;

public class AvroDeserializer {

    private CachedSchemaRegistryClient registryClient = ConsumerSelector.registryClient;

    public GenericRecord deserialize(Schema schema, byte[] data) {
        try {
            int id = -1;
            BinaryDecoder binaryEncoder = null;
            if (null != registryClient) {
                ByteBuffer buffer = getByteBuffer(data);
                id = buffer.getInt();
                int length = buffer.limit() - 1 - 4;
                schema = this.registryClient.getById(id);
                int start = buffer.position() + buffer.arrayOffset();
                binaryEncoder = DecoderFactory.get().binaryDecoder(buffer.array(), start, length, null);
            } else {
                binaryEncoder = DecoderFactory.get().binaryDecoder(data, null);
            }
            DatumReader<GenericRecord> userDatumReader = new GenericDatumReader<>(schema);
            return userDatumReader.read(null, binaryEncoder);
        } catch (Exception e) {
            throw new SerializationException(e.getMessage());
        }
    }

    public GenericRecord deserialize(Schema schema, String sendResult) {
        try {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, sendResult);
            datumReader.setSchema(schema);
            return datumReader.read(null, jsonDecoder);
        } catch (Exception e) {
            throw new SerializationException(e.getMessage());
        }
    }

    private ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != 0) {
            throw new SerializationException("Unknown magic byte!");
        }
        return buffer;
    }
}
