package stream.states.serializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.ConfUtils;
import stream.states.StreamCommon;
import stream.states.entity.FileStore;

import java.io.IOException;

@Slf4j
public class FileStoreSerializer extends StdSerializer<FileStore> {

    public FileStoreSerializer() {
        this(null);
    }

    public FileStoreSerializer(Class<FileStore> t) {
        super(t);
    }

    @Override
    public void serialize(FileStore value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        var properties = value.getProperties();
        gen.writeStartObject();
        gen.writeNumberField("writeThreadNum", ConfUtils.getInt(properties::getInt, StreamCommon.STATEMACHINE_WRITE_THREAD_NUM,
                1, log::info));
        gen.writeNumberField("readThreadNum", ConfUtils.getInt(properties::getInt, StreamCommon.STATEMACHINE_READ_THREAD_NUM,
                1, log::info));
        gen.writeNumberField("commitThreadNum", ConfUtils.getInt(properties::getInt, StreamCommon.STATEMACHINE_COMMIT_THREAD_NUM,
                1, log::info));
        gen.writeNumberField("deleteThreadNum", ConfUtils.getInt(properties::getInt, StreamCommon.STATEMACHINE_DELETE_THREAD_NUM,
                1, log::info));
        gen.writeStringField("idSupplier", value.getIdSupplier().get().toString());
        //get roots
        var list = ImmutableList.<String>builder();
        for (var dir : value.getRootSuppliers()) {
            list.add(dir.get().normalize().toString());
        }
        gen.writeObjectField("rootSuppliers", list.build());
        gen.writeObjectField("files", value.getFiles());
        gen.writeEndObject();
    }
}