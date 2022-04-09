package stream.models.lombok;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.jackson.Jacksonized;

import javax.annotation.concurrent.Immutable;

@Builder
@Data
@Immutable
@Jacksonized
public class Segment {
    @NonNull
    String relativePath;
    int segmentId;
    //where the last byte is
    //todo: make this int immutable
    @Setter
    int size;
}
