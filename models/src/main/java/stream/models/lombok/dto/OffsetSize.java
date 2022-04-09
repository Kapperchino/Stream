package stream.models.lombok.dto;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class OffsetSize {
    int offset;
    int size;
}
