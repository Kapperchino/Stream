package stream.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos.WriteReplyProto;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.function.CheckedFunction;
import stream.models.proto.record.RecordOuterClass.Record;
import stream.models.proto.requests.PublishRequestDataOuterClass.PublishRequestData;
import stream.models.proto.requests.PublishRequestHeaderOuterClass.PublishRequestHeader;
import stream.models.proto.requests.PublishRequestOuterClass.PublishRequest;
import stream.models.proto.requests.WriteRequestOuterClass.WriteRequest;
import stream.models.proto.responses.PublishResponseOuterClass.PublishResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A standalone server using raft with a configurable state machine.
 */
//todo: make config another package
@Slf4j
public class ProducerClient extends BaseClient {
    public ProducerClient(RaftGroup group, RaftProperties properties)
            throws IOException {
        super(group, properties);
    }

    public ProducerClient(RaftGroup group, RaftProperties properties, RaftPeer primaryDataStreamServer)
            throws IOException {
        super(group, properties);
    }

    public ProducerClient(RaftClient client) {
        super(client);
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT publishImpl(
            CheckedFunction<ByteString, OUTPUT, THROWABLE> sendFunction,
            List<Record> data, String topic)
            throws THROWABLE {

        List<String> keyList = data.stream().map(Record::getKey).collect(Collectors.toList());
        final var header = PublishRequestHeader.newBuilder()
                .addAllKeys(keyList)
                .setTopic(topic);

        final var requestData = PublishRequestData.newBuilder()
                .addAllData(data);

        final var publishRequest = PublishRequest.newBuilder()
                .setHeader(header)
                .setData(requestData)
                .build();

        final var request = WriteRequest.newBuilder()
                .setPublish(publishRequest)
                .build();

        return sendFunction.apply(request.toByteString());
    }

    public PublishResponse publish(List<Record> data, String topic)
            throws IOException {
        final ByteString reply = publishImpl(this::send, data, topic);
        return PublishResponse.parseFrom(reply.toByteArray());
    }

    public CompletableFuture<Long> publishAsync(List<Record> data, String topic) {
        return publishImpl(this::sendAsync, data, topic)
                .thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                        () -> WriteReplyProto.parseFrom(reply).getLength()));
    }
}
