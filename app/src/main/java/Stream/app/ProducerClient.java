package Stream.app;

import lombok.extern.slf4j.Slf4j;
import models.proto.record.RecordOuterClass.Record;
import models.proto.requests.AddPartitionRequestOuterClass.AddPartitionRequest;
import models.proto.requests.PublishRequestDataOuterClass.PublishRequestData;
import models.proto.requests.PublishRequestHeaderOuterClass.PublishRequestHeader;
import models.proto.requests.PublishRequestOuterClass.PublishRequest;
import models.proto.requests.WriteRequestOuterClass.WriteRequest;
import models.proto.responses.AddPartitionResponseOuterClass.AddPartitionResponse;
import models.proto.responses.PublishResponseOuterClass.PublishResponse;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos.WriteReplyProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedFunction;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A standalone server using raft with a configurable state machine.
 */
@Slf4j
public class ProducerClient implements Closeable {
    private final RaftClient client;

    public ProducerClient(RaftGroup group, RaftProperties properties)
            throws IOException {
        this.client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .build();
    }

    public ProducerClient(RaftGroup group, RaftProperties properties, RaftPeer primaryDataStreamServer)
            throws IOException {
        this.client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .setPrimaryDataStreamServer(primaryDataStreamServer)
                .build();
    }

    public ProducerClient(RaftClient client) {
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

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT addPartitionImpl(
            CheckedFunction<ByteString, OUTPUT, THROWABLE> sendFunction,
            String topic, long partitionId)
            throws THROWABLE {

        final var addRequest = AddPartitionRequest.newBuilder()
                .setPartition(partitionId)
                .setTopic(topic)
                .build();

        final var request = WriteRequest.newBuilder()
                .setAddPartition(addRequest)
                .build();

        return sendFunction.apply(request.toByteString());
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

    public PublishResponse publish(List<Record> data, String topic)
            throws IOException {
        final ByteString reply = publishImpl(this::send, data, topic);
        return PublishResponse.parseFrom(reply.toByteArray());
    }

    public AddPartitionResponse addPartition(String topic, long partition)
            throws IOException {
        final ByteString reply = addPartitionImpl(this::send, topic, partition);
        return AddPartitionResponse.parseFrom(reply.toByteArray());
    }

    public CompletableFuture<AddPartitionResponse> addPartitionAsync(String topic, long partition)
            throws IOException {
        return addPartitionImpl(this::sendAsync, topic, partition)
                .thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                        () -> AddPartitionResponse.parseFrom(reply)));
    }

    public CompletableFuture<Long> publishAsync(List<Record> data, String topic) {
        return publishImpl(this::sendAsync, data, topic)
                .thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                        () -> WriteReplyProto.parseFrom(reply).getLength()));
    }
}
