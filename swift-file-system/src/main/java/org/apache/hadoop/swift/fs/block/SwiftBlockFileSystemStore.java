package org.apache.hadoop.swift.fs.block;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.Block;
import org.apache.hadoop.fs.s3.FileSystemStore;
import org.apache.hadoop.fs.s3.INode;
import org.apache.hadoop.swift.fs.SwiftObjectPath;
import org.apache.hadoop.swift.fs.http.RestClient;
import org.apache.hadoop.swift.fs.snative.SwiftFileSystemStore;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * @author dmezhensky
 */
public class SwiftBlockFileSystemStore implements FileSystemStore {
    private static final String FILE_SYSTEM_VERSION_VALUE = "1";
    private static final int DEFAULT_BUFFER_SIZE = 67108864;    //64 mb
    private static final String BLOCK_PREFIX = "block_";

    private Configuration conf;

    private RestClient restClient;

    private int bufferSize;

    public void initialize(URI uri, Configuration conf) throws IOException {

        this.conf = conf;
        this.restClient = RestClient.getInstance();
        this.bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
    }

    public String getVersion() throws IOException {
        return FILE_SYSTEM_VERSION_VALUE;
    }

    private void delete(String key) throws IOException {

        restClient.delete(SwiftObjectPath.fromPath(keyToPath(key)));
    }

    public void deleteINode(Path path) throws IOException {
        delete(pathToKey(path));
    }

    public void deleteBlock(Block block) throws IOException {
        delete(blockToKey(block));
    }

    public boolean inodeExists(Path path) throws IOException {
        InputStream in = get(pathToKey(path));
        if (in == null) {
            return false;
        }
        in.close();
        return true;
    }

    public boolean blockExists(long blockId) throws IOException {
        InputStream in = get(blockToKey(blockId));
        if (in == null) {
            return false;
        }
        in.close();
        return true;
    }

    private InputStream get(String key) throws IOException {
        try {
            final InputStream inputStream = restClient.getDataAsInputStream(SwiftObjectPath.fromPath(keyToPath(key)));
            inputStream.available();
            return inputStream;
        } catch (NullPointerException e) {
            return null;
        }
    }

    private InputStream get(String key, long byteRangeStart, long length) throws IOException {

        return restClient.getDataAsInputStream(SwiftObjectPath.fromPath(keyToPath(key)), byteRangeStart, length);
    }

    public INode retrieveINode(Path path) throws IOException {
        return INode.deserialize(get(pathToKey(path)));
    }

    public File retrieveBlock(Block block, long byteRangeStart)
            throws IOException {
        File fileBlock = null;
        InputStream in = null;
        OutputStream out = null;
        try {
            fileBlock = newBackupFile();
            in = get(blockToKey(block), byteRangeStart, block.getLength() - byteRangeStart);
            out = new BufferedOutputStream(new FileOutputStream(fileBlock));
            byte[] buf = new byte[bufferSize];
            int numRead;
            while ((numRead = in.read(buf)) >= 0) {
                out.write(buf, 0, numRead);
            }
            return fileBlock;
        } catch (IOException e) {
            closeQuietly(out);
            out = null;
            if (fileBlock != null) {
                fileBlock.delete();
            }
            throw e;
        } finally {
            closeQuietly(out);
            closeQuietly(in);
        }
    }

    private File newBackupFile() throws IOException {
        File dir = new File(conf.get("hadoop.tmp.dir"));
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Cannot create Swift buffer directory: " + dir);
        }
        File result = File.createTempFile("input-", ".tmp", dir);
        result.deleteOnExit();
        return result;
    }

    public Set<Path> listSubPaths(Path path) throws IOException {
        String uri = path.toString();
        if (!uri.endsWith(Path.SEPARATOR))
            uri += Path.SEPARATOR;

        final InputStream inputStream = restClient.getDataAsInputStream(SwiftObjectPath.fromPath(path));
        final ByteArrayOutputStream data = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024 * 1024]; // 1 mb

        try {
            while (inputStream.read(buffer) > 0) {
                data.write(buffer);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final StringTokenizer tokenizer = new StringTokenizer(new String(data.toByteArray()), "\n");

        final Set<Path> paths = new HashSet<Path>();
        while (tokenizer.hasMoreTokens()) {
            paths.add(new Path(tokenizer.nextToken()));
        }

        return paths;
    }

    public Set<Path> listDeepSubPaths(Path path) throws IOException {
        String uri = path.toString();
        if (!uri.endsWith(Path.SEPARATOR))
            uri += Path.SEPARATOR;

        final byte[] buffer = restClient.findObjectsByPrefix(SwiftObjectPath.fromPath(path));
        final StringTokenizer tokenizer = new StringTokenizer(new String(buffer), "\n");

        final Set<Path> paths = new HashSet<Path>();
        while (tokenizer.hasMoreTokens()) {
            paths.add(new Path(tokenizer.nextToken()));
        }

        return paths;
    }

    private void put(String key, InputStream in, long length)
            throws IOException {

        restClient.upload(SwiftObjectPath.fromPath(keyToPath(key)), in, length);
    }

    public void storeINode(Path path, INode inode) throws IOException {
        put(pathToKey(path), inode.serialize(), inode.getSerializedLength());
    }

    public void storeBlock(Block block, File file) throws IOException {
        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(file));
            put(blockToKey(block), in, block.getLength());
        } finally {
            closeQuietly(in);
        }
    }

    public List<URI> getObjectLocation(Path path) {
        final byte[] objectLocation = restClient.getObjectLocation(SwiftObjectPath.fromPath(path));
        return SwiftFileSystemStore.extractUris(new String(objectLocation));
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private String pathToKey(Path path) {
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("Path must be absolute: " + path);
        }
        return path.toUri().getPath();
    }

    private Path keyToPath(String key) {
        return new Path(key);
    }

    private String blockToKey(long blockId) {
        return BLOCK_PREFIX + blockId;
    }

    private String blockToKey(Block block) {
        return blockToKey(block.getId());
    }

    /**
     * Deletes ALL objects from container
     * Used in testing
     *
     * @throws IOException
     */
    public void purge() throws IOException {
        final Set<Path> paths = listSubPaths(new Path("/"));
        for (Path path : paths) {
            restClient.delete(SwiftObjectPath.fromPath(path));
        }

    }

    /**
     * Dumps content of file system
     * Used for testing
     *
     * @throws IOException
     */
    public void dump() throws IOException {

        //this method is used for testing
    }
}
