package stream.states.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos.ReadReplyProto;
import org.apache.ratis.proto.ExamplesProtos.StreamWriteReplyProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachine.DataStream;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.*;
import org.apache.ratis.util.function.CheckedSupplier;
import stream.models.lombok.dto.WriteFileMeta;
import stream.models.lombok.dto.WriteResultFutures;
import stream.states.FileStoreCommon;
import stream.states.serializers.FileStoreDeserializer;
import stream.states.serializers.FileStoreSerializer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@Data
@Builder
@AllArgsConstructor
@JsonSerialize(using = FileStoreSerializer.class)
@JsonDeserialize(using = FileStoreDeserializer.class)
public class FileStore implements Closeable {
    @JsonProperty
    private final Supplier<RaftPeerId> idSupplier;
    @JsonProperty
    private final List<Supplier<Path>> rootSuppliers;
    @JsonProperty
    private final FileMap files;
    @JsonProperty
    private ExecutorService writer;
    @JsonProperty
    private ExecutorService committer;
    @JsonProperty
    private ExecutorService reader;
    @JsonProperty
    private ExecutorService deleter;
    @JsonProperty
    private RaftProperties properties;

    public FileStore(Supplier<RaftPeerId> idSupplier, RaftProperties properties) {
        this.properties = properties;
        this.idSupplier = idSupplier;
        this.rootSuppliers = new ArrayList<>();
        int writeThreadNum = ConfUtils.getInt(properties::getInt, FileStoreCommon.STATEMACHINE_WRITE_THREAD_NUM,
                1, log::info);
        int readThreadNum = ConfUtils.getInt(properties::getInt, FileStoreCommon.STATEMACHINE_READ_THREAD_NUM,
                1, log::info);
        int commitThreadNum = ConfUtils.getInt(properties::getInt, FileStoreCommon.STATEMACHINE_COMMIT_THREAD_NUM,
                1, log::info);
        int deleteThreadNum = ConfUtils.getInt(properties::getInt, FileStoreCommon.STATEMACHINE_DELETE_THREAD_NUM,
                1, log::info);
        writer = Executors.newFixedThreadPool(writeThreadNum);
        reader = Executors.newFixedThreadPool(readThreadNum);
        committer = Executors.newFixedThreadPool(commitThreadNum);
        deleter = Executors.newFixedThreadPool(deleteThreadNum);

        final List<File> dirs = ConfUtils.getFiles(properties::getFiles, FileStoreCommon.STATEMACHINE_DIR_KEY,
                null, log::info);
        Objects.requireNonNull(dirs, FileStoreCommon.STATEMACHINE_DIR_KEY + " is not set.");
        for (File dir : dirs) {
            this.rootSuppliers.add(
                    JavaUtils.memoize(() -> dir.toPath().resolve(getId().toString()).normalize().toAbsolutePath()));
        }
        this.files = new FileMap(JavaUtils.memoize(() -> idSupplier.get() + ":files"));
    }

    static Path normalize(String path) {
        Objects.requireNonNull(path, "path == null");
        return Paths.get(path).normalize();
    }

    static <T> CompletableFuture<T> submit(
            CheckedSupplier<T, IOException> task, ExecutorService executor) {
        final CompletableFuture<T> f = new CompletableFuture<>();
        executor.submit(() -> {
            try {
                f.complete(task.get());
            } catch (IOException e) {
                f.completeExceptionally(new IOException("Failed " + task, e));
            }
        });
        return f;
    }

    @JsonIgnore
    public RaftPeerId getId() {
        return Objects.requireNonNull(idSupplier.get(),
                () -> JavaUtils.getClassSimpleName(getClass()) + " is not initialized.");
    }

    private Path getRoot(@NonNull Path relative) {
        int hash = relative.toAbsolutePath().toString().hashCode() % rootSuppliers.size();
        return rootSuppliers.get(Math.abs(hash)).get();
    }

    @JsonIgnore
    public List<Path> getRoots() {
        List<Path> roots = new ArrayList<>();
        for (Supplier<Path> s : rootSuppliers) {
            roots.add(s.get());
        }
        return roots;
    }

    public Path resolve(Path relative) throws IOException {
        final Path root = getRoot(relative);
        final Path full = root.resolve(relative).normalize().toAbsolutePath();
        if (full.equals(root)) {
            throw new IOException("The file path " + relative + " resolved to " + full
                    + " is the root directory " + root);
        } else if (!full.startsWith(root)) {
            throw new IOException("The file path " + relative + " resolved to " + full
                    + " is not a sub-path under root directory " + root);
        }
        return full;
    }

    public CompletableFuture<ReadReplyProto> read(String relative, long offset, long length, boolean readCommitted) {
        final Supplier<String> name = () -> "read(" + relative
                + ", " + offset + ", " + length + ") @" + getId();
        final CheckedSupplier<ReadReplyProto, IOException> task = LogUtils.newCheckedSupplier(log, () -> {
            final FileInfo info = files.get(relative);
            final ReadReplyProto.Builder reply = ReadReplyProto.newBuilder()
                    .setResolvedPath(FileStoreCommon.toByteString(info.getRelativePath()))
                    .setOffset(offset);

            final ByteString bytes = info.read(this::resolve, offset, length, readCommitted);
            return reply.setData(bytes).build();
        }, name);
        return submit(task, reader);
    }

    public CompletableFuture<Path> delete(long index, String relative) {
        final Supplier<String> name = () -> "delete(" + relative + ") @" + getId() + ":" + index;
        final CheckedSupplier<Path, IOException> task = LogUtils.newCheckedSupplier(log, () -> {
            final FileInfo info = files.remove(relative);
            FileUtils.delete(resolve(info.getRelativePath()));
            return info.getRelativePath();
        }, name);
        return submit(task, deleter);
    }

    public CompletableFuture<Integer> submitCommit(
            long index, String relative, boolean close, long offset, int size) {
        final Function<FileInfo.UnderConstruction, FileInfo.ReadOnly> converter = close ? files::close : null;
        final FileInfo.UnderConstruction uc;
        try {
            uc = files.get(relative).asUnderConstruction();
        } catch (FileNotFoundException e) {
            return FileStoreCommon.completeExceptionally(
                    index, "Failed to write to " + relative, e);
        }
        return uc.submitCommit(offset, size, converter, committer, getId(), index);
    }

    public WriteResultFutures write(@NonNull List<WriteFileMeta> fileList) {
        if (fileList.isEmpty()) {
            return null;
        }
        var builder = WriteResultFutures.builder();
        for (var file : fileList) {
            var data = file.getData();
            var relative = file.getPath();
            var offset = file.getOffset();
            var close = file.isClose();
            var index = file.getIndex();
            var sync = file.isSync();
            final int size = data != null ? data.size() : 0;
            log.info("write {}, offset={}, size={}, close? {} @{}:{}",
                    relative, offset, size, close, getId(), index);
            final boolean createNew = offset == 0L;
            final FileInfo.UnderConstruction uc;
            if (createNew) {
                uc = new FileInfo.UnderConstruction(normalize(relative));
                files.putNew(uc);
            } else {
                try {
                    uc = files.get(relative).asUnderConstruction();
                } catch (FileNotFoundException e) {
                    log.error("Cannot find file", e);
                    return null;
                }
            }
            builder.future(size == 0 && !close ? CompletableFuture.completedFuture(0)
                    : createNew ? uc.submitCreate(this::resolve, data, close, sync, writer, getId(), index)
                    : uc.submitWrite(offset, data, close, sync, writer, getId(), index));
        }
        return builder.build();
    }

    @Override
    public void close() {
        writer.shutdownNow();
        committer.shutdownNow();
        reader.shutdownNow();
        deleter.shutdownNow();
    }

    public CompletableFuture<StreamWriteReplyProto> streamCommit(String p, long bytesWritten) {
        return CompletableFuture.supplyAsync(() -> {
            long len = 0;
            try {
                final Path full = resolve(normalize(p));
                RandomAccessFile file = new RandomAccessFile(full.toFile(), "r");
                len = file.length();
                return StreamWriteReplyProto.newBuilder().setIsSuccess(len == bytesWritten).setByteWritten(len).build();
            } catch (IOException e) {
                throw new CompletionException("Failed to commit stream write on file:" + p +
                        ", expected written bytes:" + bytesWritten + ", actual written bytes:" + len, e);
            }
        }, committer);
    }

    public CompletableFuture<?> streamLink(DataStream dataStream) {
        return CompletableFuture.supplyAsync(() -> {
            if (dataStream == null) {
                return JavaUtils.completeExceptionally(new IllegalStateException("Null stream"));
            }
            if (dataStream.getDataChannel().isOpen()) {
                return JavaUtils.completeExceptionally(
                        new IllegalStateException("DataStream: " + dataStream + " is not closed properly"));
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }, committer);
    }

    public CompletableFuture<FileStoreDataChannel> createDataChannel(String p) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                final Path full = resolve(normalize(p));
                return new FileStoreDataChannel(full);
            } catch (IOException e) {
                throw new CompletionException("Failed to create " + p, e);
            }
        }, writer);
    }

    protected static class PathDeserializer extends KeyDeserializer {
        @Override
        public Path deserializeKey(String key, DeserializationContext ctxt) {
            return Path.of(key);
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @JsonSerialize
    @JsonDeserialize
    public static class FileMap {
        @JsonProperty
        private Object name;

        @JsonDeserialize(keyUsing = PathDeserializer.class)
        @JsonSerialize()
        @JsonProperty
        private Map<Path, FileInfo> map;

        FileMap(Supplier<String> name) {
            this.name = StringUtils.stringSupplierAsObject(name);
            this.map = new ConcurrentHashMap<>();
        }

        FileMap(Supplier<String> name, Map<Path,FileInfo> map) {
            this.name = StringUtils.stringSupplierAsObject(name);
            this.map = map;
        }

        FileInfo get(String relative) throws FileNotFoundException {
            return applyFunction(relative, map::get);
        }

        FileInfo remove(String relative) throws FileNotFoundException {
            log.trace("{}: remove {}", name, relative);
            return applyFunction(relative, map::remove);
        }

        private FileInfo applyFunction(String relative, Function<Path, FileInfo> f)
                throws FileNotFoundException {
            final FileInfo info = f.apply(normalize(relative));
            if (info == null) {
                throw new FileNotFoundException("File " + relative + " not found in " + name);
            }
            return info;
        }

        void putNew(FileInfo.UnderConstruction uc) {
            log.trace("{}: putNew {}", name, uc.getRelativePath());
            CollectionUtils.putNew(uc.getRelativePath(), uc, map, name::toString);
        }

        FileInfo.ReadOnly close(FileInfo.UnderConstruction uc) {
            log.trace("{}: close {}", name, uc.getRelativePath());
            final FileInfo.ReadOnly ro = new FileInfo.ReadOnly(uc);
            CollectionUtils.replaceExisting(uc.getRelativePath(), uc, ro, map, name::toString);
            return ro;
        }
    }

    public static class FileStoreDataChannel implements StateMachine.DataChannel {
        @JsonProperty
        private final Path path;
        private final RandomAccessFile randomAccessFile;

        FileStoreDataChannel(Path path) throws FileNotFoundException {
            this.path = path;
            this.randomAccessFile = new RandomAccessFile(path.toFile(), "rw");
        }

        @Override
        public void force(boolean metadata) throws IOException {
            log.debug("force({}) at {}", metadata, path);
            randomAccessFile.getChannel().force(metadata);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            return randomAccessFile.getChannel().write(src);
        }

        @Override
        public boolean isOpen() {
            return randomAccessFile.getChannel().isOpen();
        }

        @Override
        public void close() throws IOException {
            randomAccessFile.close();
        }
    }
}
