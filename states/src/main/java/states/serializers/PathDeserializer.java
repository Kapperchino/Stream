package states.serializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;

@Slf4j
public class PathDeserializer extends StdDeserializer<Path> {

    public PathDeserializer() {
        this(null);
    }

    @Override
    public Path deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        JsonNode node = p.getCodec().readTree(p);
        var val = node.asText();
        return Path.of(val).normalize();
    }

    public PathDeserializer(Class<Path> t) {
        super(t);
    }
}