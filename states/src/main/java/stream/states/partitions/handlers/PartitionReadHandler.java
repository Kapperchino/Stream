package stream.states.partitions.handlers;

import lombok.Builder;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.AbstractMessageLite;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import stream.models.proto.requests.ConsumeRequestOuterClass;
import stream.models.proto.requests.ReadRequestOuterClass;
import stream.models.proto.responses.ConsumeResponseOuterClass;
import stream.states.handlers.ReadHandler;
import stream.states.partitions.PartitionManager;

import java.util.concurrent.CompletableFuture;

@Builder
public class PartitionReadHandler implements ReadHandler {
    PartitionManager partitionManager;

    @Override
    public CompletableFuture<ByteString> handleRead(ReadRequestOuterClass.ReadRequest request) {
        if (request.getRequestCase() != ReadRequestOuterClass.ReadRequest.RequestCase.CONSUME) {
            return null;
        }
        final ConsumeRequestOuterClass.ConsumeRequest consume = request.getConsume();
        CompletableFuture<ConsumeResponseOuterClass.ConsumeResponse> reply =
                partitionManager.readFromPartition(consume.getTopic(),
                        (int) consume.getPartition(), (int) consume.getOffset());

        return reply.thenApply(AbstractMessageLite::toByteString);
    }
}
