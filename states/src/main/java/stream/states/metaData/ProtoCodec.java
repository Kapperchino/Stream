package stream.states.metaData;

import io.scalecube.cluster.metadata.MetadataCodec;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

public class ProtoCodec implements MetadataCodec {
    @Override
    public Object deserialize(ByteBuffer byteBuffer) {
        return ByteString.copyFrom(byteBuffer);
    }

    @Override
    public ByteBuffer serialize(Object o) {
        var byteStr = (ByteString) o;
        var buf = byteStr.toByteArray();
        return ByteBuffer.wrap(buf);
    }
}
