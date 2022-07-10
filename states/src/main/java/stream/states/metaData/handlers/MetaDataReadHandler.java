package stream.states.metaData.handlers;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import stream.models.proto.requests.ReadRequestOuterClass;
import stream.states.handlers.ReadHandler;

import java.util.concurrent.CompletableFuture;

public class MetaDataReadHandler implements ReadHandler {
    @Override
    public CompletableFuture<ByteString> handleRead(ReadRequestOuterClass.ReadRequest request) {
        return null;
    }
}
