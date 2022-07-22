package stream.client;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.function.CheckedFunction;
import stream.models.proto.requests.CreateTopicRequestOuterClass;
import stream.models.proto.requests.GetClusterOuterClass;
import stream.models.proto.requests.ReadRequestOuterClass;
import stream.models.proto.requests.WriteRequestOuterClass;
import stream.models.proto.responses.CreateTopicResponseOuterClass;
import stream.models.proto.responses.GetClusterResponseOuterClass;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class MetaClient extends BaseClient {
    public MetaClient(RaftGroup group, RaftProperties properties) throws IOException {
        super(group, properties);
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT addTopicImpl(
            CheckedFunction<ByteString, OUTPUT, THROWABLE> sendFunction,
            String topic, long numPartition)
            throws THROWABLE {

        final var addRequest = CreateTopicRequestOuterClass.CreateTopicRequest.newBuilder()
                .setTopic(topic)
                .setPartitions(numPartition)
                .build();

        final var request = WriteRequestOuterClass.WriteRequest.newBuilder()
                .setCreateTopic(addRequest)
                .build();

        return sendFunction.apply(request.toByteString());
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT getClusterMeta(
            CheckedFunction<ByteString, OUTPUT, THROWABLE> sendFunction)
            throws THROWABLE {

        final var getCluster = GetClusterOuterClass.GetCluster.newBuilder()
                .build();

        final var request = ReadRequestOuterClass.ReadRequest.newBuilder()
                .setMetaData(getCluster)
                .build();

        return sendFunction.apply(request.toByteString());
    }

    public CreateTopicResponseOuterClass.CreateTopicResponse addTopic(String topic, long numPartitions) throws IOException {
        final var reply = addTopicImpl(this::sendReadOnly, topic, numPartitions);
        return CreateTopicResponseOuterClass.CreateTopicResponse.parseFrom(reply);
    }

    public CompletableFuture<CreateTopicResponseOuterClass.CreateTopicResponse> addTopicAsync(String topic, long numPartitions)
            throws IOException {
        return addTopicImpl(this::sendReadOnlyAsync, topic, numPartitions)
                .thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                        () -> CreateTopicResponseOuterClass.CreateTopicResponse.parseFrom(reply)));
    }

    public GetClusterResponseOuterClass.GetClusterResponse getClusterMeta()
            throws IOException {
        final ByteString reply = getClusterMeta(this::sendReadOnly);
        return GetClusterResponseOuterClass.GetClusterResponse.parseFrom(reply);
    }

    public CompletableFuture<GetClusterResponseOuterClass.GetClusterResponse> getClusterMetaAsync()
            throws IOException {
        return getClusterMeta(this::sendReadOnlyAsync)
                .thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                        () -> GetClusterResponseOuterClass.GetClusterResponse.parseFrom(reply)));
    }
}
