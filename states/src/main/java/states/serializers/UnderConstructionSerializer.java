package states.serializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import lombok.extern.slf4j.Slf4j;
import states.entity.FileInfo;

import java.io.IOException;

import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;

@Slf4j
public class UnderConstructionSerializer extends StdSerializer<FileInfo.UnderConstruction> {

    public UnderConstructionSerializer() {
        this(null);
    }

    public UnderConstructionSerializer(Class<FileInfo.UnderConstruction> t) {
        super(t);
    }

    @Override
    public void serialize(FileInfo.UnderConstruction value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStringField("type", "UnderConstruction");
        gen.writeStringField("relativePath", value.getRelativePath().normalize().toString());
        gen.writeObjectField("writeInfoMap", value.getWriteInfoMap());
        gen.writeNumberField("lastWriteIndex", value.getLastWriteIndex().get());
        gen.writeNumberField("writeSize", value.getWriteSize());
        gen.writeNumberField("committedSize", value.getCommittedSize());
    }

    @Override
    public void serializeWithType(FileInfo.UnderConstruction value, JsonGenerator gen,
                                  SerializerProvider provider, TypeSerializer typeSer)
            throws IOException, JsonProcessingException {
        WritableTypeId typeId = typeSer.typeId(value, START_OBJECT);
        typeSer.writeTypePrefix(gen, typeId);
        serialize(value, gen, provider);
        typeSer.writeTypeSuffix(gen, typeId);
    }
}