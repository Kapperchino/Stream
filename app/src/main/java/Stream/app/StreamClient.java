package Stream.app;

import lombok.extern.slf4j.Slf4j;
import models.proto.record.RecordOuterClass.Record;
import models.proto.requests.PublishRequestDataOuterClass.PublishRequestData;
import models.proto.requests.PublishRequestHeaderOuterClass.PublishRequestHeader;
import models.proto.requests.PublishRequestOuterClass.PublishRequest;
import models.proto.responses.PublishResponseOuterClass.PublishResponse;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos.ReadRequestProto;
import org.apache.ratis.proto.ExamplesProtos.WriteReplyProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.function.CheckedFunction;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * A standalone server using raft with a configurable state machine.
 */
@Slf4j
public class StreamClient implements Closeable {
    private final RaftClient client;

    public StreamClient(RaftGroup group, RaftProperties properties)
            throws IOException {
        this.client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .build();
    }

    public StreamClient(RaftGroup group, RaftProperties properties, RaftPeer primaryDataStreamServer)
            throws IOException {
        this.client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .setPrimaryDataStreamServer(primaryDataStreamServer)
                .build();
    }

    public StreamClient(RaftClient client) {
        this.client = client;
    }

    static ByteString send(
            ByteString request, CheckedFunction<Message, RaftClientReply, IOException> sendFunction)
            throws IOException {
        final RaftClientReply reply = sendFunction.apply(Message.valueOf(request));
        final StateMachineException sme = reply.getStateMachineException();
        if (sme != null) {
            throw new IOException("Failed to send request " + request, sme);
        }
        Preconditions.assertTrue(reply.isSuccess(), () -> "Failed " + request + ", reply=" + reply);
        return reply.getMessage().getContent();
    }

    static CompletableFuture<ByteString> sendAsync(
            ByteString request, Function<Message, CompletableFuture<RaftClientReply>> sendFunction) {
        return sendFunction.apply(() -> request
        ).thenApply(reply -> {
            final StateMachineException sme = reply.getStateMachineException();
            if (sme != null) {
                throw new CompletionException("Failed to send request " + request, sme);
            }
            Preconditions.assertTrue(reply.isSuccess(), () -> "Failed " + request + ", reply=" + reply);
            return reply.getMessage().getContent();
        });
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT publishImpl(
            CheckedFunction<ByteString, OUTPUT, THROWABLE> sendFunction,
            String key, List<Record> data, String topic)
            throws THROWABLE {

        final var header = PublishRequestHeader.newBuilder()
                .setKey(key)
                .setTopic(topic);

        final var requestData = PublishRequestData.newBuilder()
                .addAllData(data);

        final var request = PublishRequest.newBuilder()
                .setHeader(header)
                .setData(requestData)
                .build();

        return sendFunction.apply(ByteString.copyFrom(request.toByteArray()));
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private ByteString send(ByteString request) throws IOException {
        return send(request, client.io()::send);
    }

    private ByteString sendReadOnly(ByteString request) throws IOException {
        return send(request, client.io()::sendReadOnly);
    }

    private CompletableFuture<ByteString> sendAsync(ByteString request) {
        return sendAsync(request, client.async()::send);
    }

    private CompletableFuture<ByteString> sendReadOnlyAsync(ByteString request) {
        return sendAsync(request, client.async()::sendReadOnly);
    }

    public PublishResponse publish(String key, List<Record> data, String topic)
            throws IOException {
        final ByteString reply = publishImpl(this::send, key, data, topic);
        return PublishResponse.parseFrom(reply.toByteArray());
    }

    public CompletableFuture<Long> publishAsync(String key, List<Record> data, String topic) {
        return publishImpl(this::sendAsync, key, data, topic)
                .thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                        () -> WriteReplyProto.parseFrom(reply).getLength()));
    }
}
