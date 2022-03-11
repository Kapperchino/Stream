package models.lombok;

import lombok.Builder;
import lombok.Data;
import lombok.Value;

@Builder
@Value
public class RecordInfo {
    int offset;
    int fileOffset;
    int size;
    int segmentId;
}
