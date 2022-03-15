package models.lombok.dto;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Value
@Builder
public class WriteResultFutures {
    @Singular
    List<CompletableFuture<Integer>> futures;
}
