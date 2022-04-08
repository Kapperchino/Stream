package models.lombok;

import lombok.*;
import lombok.extern.jackson.Jacksonized;

import javax.annotation.concurrent.Immutable;
import java.nio.file.Path;

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
