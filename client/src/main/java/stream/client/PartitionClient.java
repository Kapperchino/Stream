package stream.client;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.function.CheckedFunction;
import stream.models.proto.requests.AddPartitionRequestOuterClass;
import stream.models.proto.requests.WriteRequestOuterClass;
import stream.models.proto.responses.AddPartitionResponseOuterClass;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class PartitionClient extends BaseClient {

    public PartitionClient(RaftGroup group, RaftProperties properties)
            throws IOException {
        super(group, properties);
    }

    public PartitionClient(RaftGroup group, RaftProperties properties, RaftPeer primaryDataStreamServer)
            throws IOException {
        super(group, properties);
    }

    public PartitionClient(RaftClient client) {
        super(client);
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT addPartitionImpl(
            CheckedFunction<ByteString, OUTPUT, THROWABLE> sendFunction,
            String topic, long partitionId)
            throws THROWABLE {

        final var addRequest = AddPartitionRequestOuterClass.AddPartitionRequest.newBuilder()
                .setPartition(partitionId)
                .setTopic(topic)
                .build();

        final var request = WriteRequestOuterClass.WriteRequest.newBuilder()
                .setAddPartition(addRequest)
                .build();

        return sendFunction.apply(request.toByteString());
    }

    public AddPartitionResponseOuterClass.AddPartitionResponse addPartition(String topic, long partition)
            throws IOException {
        final ByteString reply = addPartitionImpl(this::send, topic, partition);
        return AddPartitionResponseOuterClass.AddPartitionResponse.parseFrom(reply);
    }

    public CompletableFuture<AddPartitionResponseOuterClass.AddPartitionResponse> addPartitionAsync(String topic, long partition)
            throws IOException {
        return addPartitionImpl(this::sendAsync, topic, partition)
                .thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                        () -> AddPartitionResponseOuterClass.AddPartitionResponse.parseFrom(reply)));
    }

}
