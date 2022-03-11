package models.lombok;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import javax.annotation.concurrent.Immutable;
import java.nio.file.Path;

@Builder
@Value
@Immutable
public class Segment {
    @NonNull
    Path relativePath;
    int segmentId;
}
