package models.lombok;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class RecordInfo {
    int offset;
    int fileOffset;
    int size;
    int segmentId;
}
