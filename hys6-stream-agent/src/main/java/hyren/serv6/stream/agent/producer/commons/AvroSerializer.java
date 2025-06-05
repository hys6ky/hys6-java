package hyren.serv6.stream.agent.producer.commons;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public class AvroSerializer implements Serializer<GenericRecord> {

    protected CachedSchemaRegistryClient registryClient = null;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Object urls = configs.get("schema.registry.url");
        if (null != urls && !"".equals(urls)) {
            registryClient = new CachedSchemaRegistryClient(urls.toString(), 10000);
        }
    }

    @Override
    public byte[] serialize(String topic, GenericRecord genericRecord) {
        DatumWriter<GenericRecord> userDatumWriter = new SpecificDatumWriter<>(genericRecord.getSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
        try {
            if (null != registryClient) {
                outputStream.write(0);
                int id = registryClient.register(topic + "-value", genericRecord.getSchema());
                outputStream.write(ByteBuffer.allocate(4).putInt(id).array());
            }
            userDatumWriter.write(genericRecord, binaryEncoder);
            byte[] bytes = outputStream.toByteArray();
            outputStream.close();
            return bytes;
        } catch (Exception e) {
            throw new SerializationException(e.getMessage());
        }
    }

    @Override
    public void close() {
    }
}
