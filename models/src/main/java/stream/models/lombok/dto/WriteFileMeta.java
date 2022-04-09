package stream.models.lombok.dto;

import com.google.protobuf.ByteString;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class WriteFileMeta {
    int index;
    String path;
    boolean close;
    boolean sync;
    long offset;
    ByteString data;

    public FileWrittenMeta getFileWritten() {
        return FileWrittenMeta.builder()
                .index(index)
                .path(path)
                .close(close)
                .sync(sync)
                .offset(offset)
                .build();
    }

    public FileWrittenMeta getFileWritten(long size) {
        return FileWrittenMeta.builder()
                .index(index)
                .path(path)
                .close(close)
                .sync(sync)
                .offset(offset)
                .size(size)
                .build();
    }
}
