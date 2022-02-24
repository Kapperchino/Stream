package models.lombok;

import lombok.Builder;
import lombok.Data;

import java.nio.file.Path;

@Builder
@Data
public class Segment {
    Path relativePath;
}
