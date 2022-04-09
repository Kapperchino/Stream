package stream.states.serializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import lombok.extern.slf4j.Slf4j;
import stream.states.entity.FileInfo;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class UnderConstructionDeserializer extends StdDeserializer<FileInfo.UnderConstruction> {

    public UnderConstructionDeserializer() {
        this(null);
    }

    @Override
    public FileInfo.UnderConstruction deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        JsonNode node = p.getCodec().readTree(p);
        var builder = FileInfo.UnderConstruction.builder();
        builder.relativePath(Path.of(node.get("relativePath").asText()));
        builder.writeInfoMap(new ConcurrentHashMap<>());
        builder.lastWriteIndex(new AtomicLong(node.get("lastWriteIndex").asInt()));
        builder.writeSize(node.get("writeSize").asInt());
        builder.committedSize(node.get("committedSize").asInt());
        return builder.build();
    }

    public UnderConstructionDeserializer(Class<FileInfo.UnderConstruction> t) {
        super(t);
    }
}