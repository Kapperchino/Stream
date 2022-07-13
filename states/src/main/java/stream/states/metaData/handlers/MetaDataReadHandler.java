package stream.states.metaData.handlers;

import lombok.Builder;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import stream.models.proto.requests.ReadRequestOuterClass;
import stream.models.proto.responses.GetClusterResponseOuterClass.GetClusterResponse;
import stream.models.proto.responses.GetTopicsResponseOuterClass.GetTopicsResponse;
import stream.states.handlers.ReadHandler;
import stream.states.metaData.MetaManager;

import java.util.concurrent.CompletableFuture;

@Builder
public class MetaDataReadHandler implements ReadHandler {
    MetaManager manager;

    @Override
    public CompletableFuture<ByteString> handleRead(ReadRequestOuterClass.ReadRequest request) {
        switch (request.getRequestCase()) {
            case METADATA:
                return getClusterMeta();
            case TOPICS:
                return getTopics();
            default:
                return null;
        }
    }

    private CompletableFuture<ByteString> getTopics() {
        return manager.getTopics().thenApply((topics) -> {
            var builder = GetTopicsResponse.newBuilder();
            topics.forEach(val -> builder.putTopics(val.getTopicName(), val));
            return builder.build().toByteString();
        });
    }

    private CompletableFuture<ByteString> getClusterMeta() {
        return manager.getClusterMeta().thenApply((meta) ->
                GetClusterResponse.newBuilder()
                        .setMeta(meta).build().toByteString());
    }
}

