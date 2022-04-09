package stream.client;

import lombok.SneakyThrows;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.function.CheckedFunction;
import stream.models.proto.requests.ConsumeRequestOuterClass;
import stream.models.proto.requests.ReadRequestOuterClass;
import stream.models.proto.responses.ConsumeResponseOuterClass;

import java.io.IOException;

public class ConsumerClient extends BaseClient {

    public ConsumerClient(RaftGroup group, RaftProperties properties)
            throws IOException {
        super(group, properties);
    }

    public ConsumerClient(RaftGroup group, RaftProperties properties, RaftPeer primaryDataStreamServer)
            throws IOException {
        super(group, properties);
    }

    public ConsumerClient(RaftClient client) {
        super(client);
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT readImpl(
            CheckedFunction<ByteString, OUTPUT, THROWABLE> sendFunction,
            String topic, long partition, long offset)
            throws THROWABLE {
        final var consume = ConsumeRequestOuterClass.ConsumeRequest.newBuilder()
                .setOffset(offset)
                .setPartition(partition)
                .setTopic(topic)
                .build();

        final var request = ReadRequestOuterClass.ReadRequest.newBuilder()
                .setConsume(consume)
                .build();

        return sendFunction.apply(request.toByteString());
    }

    @SneakyThrows
    public ConsumeResponseOuterClass.ConsumeResponse readPartition(long offset, long partition, String topic) {
        final ByteString reply = readImpl(this::sendReadOnly, topic, partition, offset);
        return ConsumeResponseOuterClass.ConsumeResponse.parseFrom(reply);
    }

}
