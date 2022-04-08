package states.serializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.ConfUtils;
import states.FileStoreCommon;
import states.entity.FileStore;

import java.io.IOException;
import java.nio.file.Path;

@Slf4j
public class PathSerializer extends StdSerializer<Path> {

    public PathSerializer() {
        this(null);
    }

    public PathSerializer(Class<Path> t) {
        super(t);
    }

    @Override
    public void serialize(Path value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeObject(value.normalize().toString());
    }
}