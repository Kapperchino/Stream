package models.lombok;

import lombok.*;

import javax.annotation.concurrent.Immutable;
import java.nio.file.Path;

@Builder
@Data
@Immutable
public class Segment {
    @NonNull
    Path relativePath;
    int segmentId;
    //where the last byte is
    //todo: make this int immutable
    @Setter
    int size;
}
