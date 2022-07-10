package stream.states.metaData.handlers;

import org.apache.ratis.proto.RaftProtos;
import stream.models.proto.requests.WriteRequestOuterClass;
import stream.states.handlers.WriteHandler;

import java.util.concurrent.CompletableFuture;

public class MetaDataWriteHandler implements WriteHandler {
    @Override
    public CompletableFuture<?> handleWrite(WriteRequestOuterClass.WriteRequest request, RaftProtos.LogEntryProto entry) {
        return null;
    }
}
