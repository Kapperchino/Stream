package stream.states.entity;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TaskQueue;
import org.apache.ratis.util.function.CheckedFunction;
import org.apache.ratis.util.function.CheckedSupplier;
import stream.states.StreamCommon;
import stream.states.serializers.UnderConstructionDeserializer;
import stream.states.serializers.UnderConstructionSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@SuperBuilder
@NoArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = FileInfo.UnderConstruction.class, name = "UnderConstruction"),
        @JsonSubTypes.Type(value = FileInfo.ReadOnly.class, name = "ReadOnly")
})
public abstract class FileInfo {
    @JsonProperty
    protected Path relativePath;

    FileInfo(Path relativePath) {
        this.relativePath = relativePath;
    }

    Path getRelativePath() {
        return relativePath;
    }

    long getWriteSize() {
        throw new UnsupportedOperationException(
                "File " + getRelativePath() + " size is unknown.");
    }

    long getCommittedSize() {
        throw new UnsupportedOperationException(
                "File " + getRelativePath() + " size is unknown.");
    }

    ByteString read(CheckedFunction<Path, Path, IOException> resolver, long offset, long length, boolean readCommitted)
            throws IOException {
        if (readCommitted && offset + length > getCommittedSize()) {
            throw new IOException("Failed to read Committed: offset (=" + offset
                    + " + length (=" + length + ") > size = " + getCommittedSize()
                    + ", path=" + getRelativePath());
        } else if (offset + length > getWriteSize()) {
            throw new IOException("Failed to read Wrote: offset (=" + offset
                    + " + length (=" + length + ") > size = " + getWriteSize()
                    + ", path=" + getRelativePath());
        }
        try (SeekableByteChannel in = Files.newByteChannel(
                resolver.apply(getRelativePath()), StandardOpenOption.READ)) {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(StreamCommon.getChunkSize(length));
            in.position(offset).read(buffer);
            buffer.flip();
            return ByteString.copyFrom(buffer);
        }
    }

    UnderConstruction asUnderConstruction() {
        throw new UnsupportedOperationException(
                "File " + getRelativePath() + " is not under construction.");
    }

    @AllArgsConstructor
    @SuperBuilder
    @JsonDeserialize(builder = ReadOnly.ReadOnlyBuilder.class)
    @JsonTypeName("ReadOnly")
    static class ReadOnly extends FileInfo {
        @JsonProperty
        private final long committedSize;
        @JsonProperty
        private final long writeSize;

        ReadOnly(UnderConstruction f) {
            super(f.getRelativePath());
            this.committedSize = f.getCommittedSize();
            this.writeSize = f.getWriteSize();
        }

        @Override
        long getCommittedSize() {
            return committedSize;
        }

        @Override
        long getWriteSize() {
            return writeSize;
        }
    }

    @Builder
    @AllArgsConstructor
    @JsonDeserialize(builder = WriteInfo.WriteInfoBuilder.class)
    public static class WriteInfo {
        /**
         * Future to make sure that each commit is executed after the corresponding write.
         */
        private final CompletableFuture<Integer> writeFuture;
        /**
         * Future to make sure that each commit is executed after the previous commit.
         */
        private final CompletableFuture<Integer> commitFuture;
        /**
         * Previous commit index.
         */
        @JsonProperty
        private final long previousIndex;

        WriteInfo(CompletableFuture<Integer> writeFuture, long previousIndex) {
            this.writeFuture = writeFuture;
            this.commitFuture = new CompletableFuture<>();
            this.previousIndex = previousIndex;
        }

        CompletableFuture<Integer> getCommitFuture() {
            return commitFuture;
        }

        CompletableFuture<Integer> getWriteFuture() {
            return writeFuture;
        }

        long getPreviousIndex() {
            return previousIndex;
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @AllArgsConstructor
    @SuperBuilder
    @Data
    @JsonDeserialize(using = UnderConstructionDeserializer.class)
    @JsonSerialize(using = UnderConstructionSerializer.class)
    @JsonTypeName("UnderConstruction")
    public static class UnderConstruction extends FileInfo {
        /**
         * A queue to make sure that the writes are in order.
         */
        private TaskQueue writeQueue = new TaskQueue("writeQueue");
        @JsonProperty
        private Map<Long, WriteInfo> writeInfoMap = new ConcurrentHashMap<>();
        private AtomicLong lastWriteIndex = new AtomicLong(-1L);
        @JsonIgnore
        private FileStore.FileStoreDataChannel out;
        /**
         * The size written to a local file.
         */
        @JsonProperty
        private volatile long writeSize;
        /**
         * The size committed to client.
         */
        @JsonProperty
        private volatile long committedSize;

        UnderConstruction(Path relativePath) {
            super(relativePath);
        }

        @Override
        UnderConstruction asUnderConstruction() {
            return this;
        }

        @Override
        public long getCommittedSize() {
            return committedSize;
        }

        @Override
        public long getWriteSize() {
            return writeSize;
        }

        public Path getRelativePath() {
            return relativePath;
        }

        CompletableFuture<Integer> submitCreate(
                CheckedFunction<Path, Path, IOException> resolver, ByteString data, boolean close, boolean sync,
                ExecutorService executor, RaftPeerId id, long index) {
            final Supplier<String> name = () -> "create(" + getRelativePath() + ", "
                    + close + ") @" + id + ":" + index;
            final CheckedSupplier<Integer, IOException> task = LogUtils.newCheckedSupplier(log, () -> {
                if (out == null) {
                    out = new FileStore.FileStoreDataChannel(resolver.apply(getRelativePath()));
                }
                return write(0L, data, close, sync);
            }, name);
            return submitWrite(task, executor, id, index);
        }

        CompletableFuture<Integer> submitWrite(
                long offset, ByteString data, boolean close, boolean sync, ExecutorService executor,
                RaftPeerId id, long index) {
            final Supplier<String> name = () -> "write(" + getRelativePath() + ", "
                    + offset + ", " + close + ") @" + id + ":" + index;
            final CheckedSupplier<Integer, IOException> task = LogUtils.newCheckedSupplier(log,
                    () -> write(offset, data, close, sync), name);
            return submitWrite(task, executor, id, index);
        }

        private CompletableFuture<Integer> submitWrite(
                CheckedSupplier<Integer, IOException> task,
                ExecutorService executor, RaftPeerId id, long index) {
            final CompletableFuture<Integer> f = writeQueue.submit(task, executor,
                    e -> new IOException("Failed " + task, e));
            final WriteInfo info = new WriteInfo(f, lastWriteIndex.getAndSet(index));
            writeInfoMap.put(index, info);
            return f;
        }

        private int write(long offset, ByteString data, boolean close, boolean sync) throws IOException {
            // If leader finish write data with offset = 4096 and writeSize become 8192,
            // and 2 follower has not written the data with offset = 4096,
            // then start a leader election. So client will retry send the data with offset = 4096,
            // then offset < writeSize in leader.
            if (offset < writeSize) {
                return data.size();
            }
            if (offset != writeSize) {
                throw new IOException("Offset/size mismatched: offset = " + offset
                        + " != writeSize = " + writeSize + ", path=" + getRelativePath());
            }
            if (out == null) {
                throw new IOException("File output is not initialized, path=" + getRelativePath());
            }

            synchronized (out) {
                int n = 0;
                if (data != null) {
                    final ByteBuffer buffer = data.asReadOnlyByteBuffer();
                    try {
                        for (; buffer.remaining() > 0; ) {
                            n += out.write(buffer);
                        }
                    } finally {
                        writeSize += n;
                    }
                }

                if (sync) {
                    out.force(false);
                }

                if (close) {
                    out.close();
                }
                return n;
            }
        }

        @SneakyThrows
        CompletableFuture<Integer> submitCommit(
                long offset, int size, Function<UnderConstruction, ReadOnly> closeFunction,
                ExecutorService executor, RaftPeerId id, long index) {
            final boolean close = closeFunction != null;
            final Supplier<String> name = () -> "commit(" + getRelativePath() + ", "
                    + offset + ", " + size + ", close? " + close + ") @" + id + ":" + index;

            final WriteInfo info = writeInfoMap.get(index);
            if (info == null) {
                return JavaUtils.completeExceptionally(
                        new IOException(name.get() + " is already committed."));
            }

            final CheckedSupplier<Integer, IOException> task = LogUtils.newCheckedSupplier(log, () -> {
                if (offset != committedSize) {
                    throw new IOException("Offset/size mismatched: offset = "
                            + offset + " != committedSize = " + committedSize
                            + ", path=" + getRelativePath());
                } else if (committedSize + size > writeSize) {
                    throw new IOException("Offset/size mismatched: committed (=" + committedSize
                            + ") + size (=" + size + ") > writeSize = " + writeSize);
                }
                committedSize += size;

                if (close) {
                    ReadOnly ignored = closeFunction.apply(this);
                    writeInfoMap.remove(index);
                }
                info.getCommitFuture().complete(size);
                return size;
            }, name);

            // Remove previous info, if there is any.
            final WriteInfo previous = writeInfoMap.remove(info.getPreviousIndex());
            final CompletableFuture<Integer> previousCommit = previous != null ?
                    previous.getCommitFuture() : CompletableFuture.completedFuture(0);
            // Commit after both current write and previous commit completed.
            return info.getWriteFuture().thenCombineAsync(previousCommit, (wSize, previousCommitSize) -> {
                Preconditions.assertTrue(size == wSize);
                try {
                    return task.get();
                } catch (IOException e) {
                    throw new CompletionException("Failed " + task, e);
                }
            }, executor);
        }
    }
}
