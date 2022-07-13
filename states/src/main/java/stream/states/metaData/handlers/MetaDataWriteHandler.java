package stream.states.metaData.handlers;

import lombok.Builder;
import org.apache.ratis.proto.RaftProtos;
import stream.models.proto.requests.WriteRequestOuterClass;
import stream.states.handlers.WriteHandler;
import stream.states.metaData.MetaManager;

import java.util.concurrent.CompletableFuture;

@Builder
public class MetaDataWriteHandler implements WriteHandler {
    MetaManager manager;
    @Override
    public CompletableFuture<?> handleWrite(WriteRequestOuterClass.WriteRequest request, RaftProtos.LogEntryProto entry) {
        return null;
    }
}
