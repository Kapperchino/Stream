package models.lombok.dto;

import com.google.protobuf.ByteString;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class FileWrittenMeta {
    int index;
    String path;
    boolean close;
    boolean sync;
    long offset;
}
