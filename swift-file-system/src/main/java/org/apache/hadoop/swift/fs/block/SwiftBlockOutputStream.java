package org.apache.hadoop.swift.fs.block;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.Block;
import org.apache.hadoop.fs.s3.FileSystemStore;
import org.apache.hadoop.fs.s3.INode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Wraps OutputStream for streaming data into Swift directly
 */
public class SwiftBlockOutputStream extends OutputStream {
    private Configuration conf;

    private int bufferSize;

    private FileSystemStore store;

    private Path path;

    private long blockSize;

    private File backupFile;

    private OutputStream backupStream;

    private Random r = new Random();

    private boolean closed;

    private int pos = 0;

    private long filePos = 0;

    private int bytesWrittenToBlock = 0;

    private byte[] outBuf;

    private List<Block> blocks = new ArrayList<Block>();

    private Block nextBlock;

    public SwiftBlockOutputStream(Configuration conf, FileSystemStore store, Path path, long blockSize,
                                  int buffersize) throws IOException {
        this.conf = conf;
        this.store = store;
        this.path = path;
        this.blockSize = blockSize;
        this.backupFile = newBackupFile();
        this.backupStream = new FileOutputStream(backupFile);
        this.bufferSize = buffersize;
        this.outBuf = new byte[bufferSize];
    }

    private File newBackupFile() throws IOException {
        File dir = new File(conf.get("hadoop.tmp.dir"));
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Cannot create Swift buffer directory: " + dir);
        }
        File result = File.createTempFile("output-", ".tmp", dir);
        result.deleteOnExit();
        return result;
    }

    public long getPos() throws IOException {
        return filePos;
    }

    @Override
    public synchronized void write(int b) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }

        if ((bytesWrittenToBlock + pos == blockSize) || (pos >= bufferSize)) {
            flush();
        }
        outBuf[pos++] = (byte) b;
        filePos++;
    }

    @Override
    public synchronized void write(byte b[], int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        while (len > 0) {
            int remaining = bufferSize - pos;
            int toWrite = Math.min(remaining, len);
            System.arraycopy(b, off, outBuf, pos, toWrite);
            pos += toWrite;
            off += toWrite;
            len -= toWrite;
            filePos += toWrite;

            if ((bytesWrittenToBlock + pos >= blockSize) || (pos == bufferSize)) {
                flush();
            }
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }

        if (bytesWrittenToBlock + pos >= blockSize) {
            flushData((int) blockSize - bytesWrittenToBlock);
        }
        if (bytesWrittenToBlock == blockSize) {
            endBlock();
        }
        flushData(pos);
    }

    private synchronized void flushData(int maxPos) throws IOException {
        int workingPos = Math.min(pos, maxPos);

        if (workingPos > 0) {
            //
            // To the local block backup, write just the bytes
            //
            backupStream.write(outBuf, 0, workingPos);

            //
            // Track position
            //
            bytesWrittenToBlock += workingPos;
            System.arraycopy(outBuf, workingPos, outBuf, 0, pos - workingPos);
            pos -= workingPos;
        }
    }

    private synchronized void endBlock() throws IOException {
        //
        // Done with local copy
        //
        backupStream.close();

        //
        // Send it to Swift
        nextBlockOutputStream();
        store.storeBlock(nextBlock, backupFile);
        internalClose();

        //
        // Delete local backup, start new one
        //
        backupFile.delete();
        backupFile = newBackupFile();
        backupStream = new FileOutputStream(backupFile);
        bytesWrittenToBlock = 0;
    }

    private synchronized void nextBlockOutputStream() throws IOException {
        long blockId = r.nextLong();
        while (store.blockExists(blockId)) {
            blockId = r.nextLong();
        }
        nextBlock = new Block(blockId, bytesWrittenToBlock);
        blocks.add(nextBlock);
        bytesWrittenToBlock = 0;
    }

    private synchronized void internalClose() throws IOException {
        INode inode = new INode(INode.FILE_TYPES[1], blocks.toArray(new Block[blocks.size()]));
        store.storeINode(path, inode);
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }

        flush();
        if (filePos == 0 || bytesWrittenToBlock != 0) {
            endBlock();
        }

        backupStream.close();
        backupFile.delete();

        super.close();

        closed = true;
    }
}
