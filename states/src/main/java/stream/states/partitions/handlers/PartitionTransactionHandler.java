package stream.states.partitions.handlers;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import stream.models.proto.requests.*;
import stream.states.StreamCommon;
import stream.states.handlers.TransactionHandler;
import stream.states.partitions.PartitionManager;

import java.util.concurrent.CompletableFuture;

@Builder
@Slf4j
public class PartitionTransactionHandler implements TransactionHandler {
    PartitionManager partitionManager;

    @Override
    public TransactionContext startTransaction(RaftClientRequest request, WriteRequestOuterClass.WriteRequest proto, TransactionContext.Builder contextBuilder) {
        if (proto.getRequestCase() == WriteRequestOuterClass.WriteRequest.RequestCase.PUBLISH) {
            var publishProto = proto.getPublish();
            final WriteRequestOuterClass.WriteRequest newProto = WriteRequestOuterClass.WriteRequest.newBuilder()
                    .setPublish(PublishRequestOuterClass.PublishRequest.newBuilder().setHeader(publishProto.getHeader())).build();
            contextBuilder.setLogData(ByteString.copyFrom(newProto.toByteArray())).setStateMachineData(ByteString.copyFrom(publishProto.getData().toByteArray()));
            return contextBuilder.build();
        }
        return null;
    }

    @Override
    public CompletableFuture<Message> applyTransaction(WriteRequestOuterClass.WriteRequest request, long index) {
        switch (request.getRequestCase()) {
            case PUBLISH:
                //TODO: add recovery features, currently when the state machines are down we lose all meta-data
                return writeCommit(index, request.getPublish().getHeader(), request.getPublish().getData());
            case ADDPARTITION:
                return addPartition(index, request.getAddPartition());
            case CREATETOPIC:
                break;
            default:
                return null;
        }
        return null;
    }

    private CompletableFuture<Message> writeCommit(
            long index, PublishRequestHeaderOuterClass.PublishRequestHeader header, PublishRequestDataOuterClass.PublishRequestData data) {
        var f1 = partitionManager
                .submitCommit(index, header, data);
        if (f1 != null) {
            return f1.thenApply(reply -> Message.valueOf(reply.toByteString()));
        }
        return StreamCommon.completeExceptionally(
                index, "Failed to commit, index: " + index);
    }

    private CompletableFuture<Message> addPartition(
            long index, AddPartitionRequestOuterClass.AddPartitionRequest request) {
        var f1 = partitionManager.addPartition(index, request);
        if (f1 != null) {
            return f1.thenApply(reply -> Message.valueOf(reply.toByteString()));
        }
        return null;
    }
}
