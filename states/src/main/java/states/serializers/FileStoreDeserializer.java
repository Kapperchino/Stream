package states.serializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.JavaUtils;
import states.entity.FileStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

@Slf4j
public class FileStoreDeserializer extends StdDeserializer<FileStore> {

    public FileStoreDeserializer() {
        this(null);
    }

    public FileStoreDeserializer(Class<FileStore> t) {
        super(t);
    }

    @Override
    public FileStore deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        JsonNode node = p.getCodec().readTree(p);
        var builder = FileStore.builder();
        builder.writer(Executors.newFixedThreadPool(node.get("writeThreadNum").asInt()));
        builder.reader(Executors.newFixedThreadPool(node.get("readThreadNum").asInt()));
        builder.committer(Executors.newFixedThreadPool(node.get("commitThreadNum").asInt()));
        builder.deleter(Executors.newFixedThreadPool(node.get("deleteThreadNum").asInt()));
        builder.idSupplier(JavaUtils.memoize(() -> RaftPeerId.valueOf(node.get("idSupplier").asText())));
        var dirs = node.get("rootSuppliers");
        var list = ImmutableList.<Supplier<Path>>builder();
        for (var dir : dirs) {
            var dirVal = dir.asText();
            list.add(JavaUtils.memoize(() -> Path.of(dirVal)));
        }
        builder.rootSuppliers(list.build());
        var files = ctxt.readTreeAsValue(node.get("files"), FileStore.FileMap.class);
        builder.files(files);
        return builder.build();
    }
}