package stream.models.lombok;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Builder
@Value
@Jacksonized
public class RecordInfo {
    int offset;
    int fileOffset;
    int size;
    int segmentId;
}
