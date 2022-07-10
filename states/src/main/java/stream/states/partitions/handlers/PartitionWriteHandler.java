package stream.states.partitions.handlers;

import lombok.Builder;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import stream.models.proto.requests.PublishRequestDataOuterClass;
import stream.models.proto.requests.WriteRequestOuterClass;
import stream.states.FileStoreCommon;
import stream.states.handlers.WriteHandler;
import stream.states.partitions.PartitionManager;

import java.util.concurrent.CompletableFuture;

@Builder
public class PartitionWriteHandler implements WriteHandler {
    PartitionManager manager;

    @Override
    public CompletableFuture<?> handleWrite(WriteRequestOuterClass.WriteRequest request, RaftProtos.LogEntryProto entry) {
        if (request.getRequestCase() == WriteRequestOuterClass.WriteRequest.RequestCase.PUBLISH) {
            var smLog = entry.getStateMachineLogEntry();
            var publishReq = request.getPublish();
            var machineData = smLog.getStateMachineEntry().getStateMachineData();
            PublishRequestDataOuterClass.PublishRequestData publishData = null;
            try {
                publishData = PublishRequestDataOuterClass.PublishRequestData.parseFrom(machineData);
            } catch (InvalidProtocolBufferException e) {
                return FileStoreCommon.completeExceptionally(
                        entry.getIndex(), "Failed to parse data, entry=" + entry, e);
            }
            return manager.writeToPartition(entry.getIndex(), publishReq.getHeader().getTopic(), 0, publishData);
        }
        return null;
    }
}
