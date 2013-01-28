package org.apache.hadoop.fs.swift.snative;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.util.SwiftUtils;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;

/**
 * Class for functional testing huge file upload to Swift FS.
 */
public class SwiftFileSystemForIntegrationTests extends SwiftNativeFileSystem {
  private SwiftNativeFileSystemStore store;

  public void initialize(URI swiftUri, Configuration configuration) throws IOException {
    super.initialize(swiftUri, configuration);
    setConf(configuration);
    if (store == null) {
      store = new SwiftNativeFileSystemStore();
    }

    store.initialize(swiftUri, configuration);
  }

  @Override
  public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite,
                                   int bufferSize, short replication, long blockSize,
                                   Progressable progress) throws IOException {

    FileStatus fileStatus = null;
    try {
      fileStatus = getFileStatus(makeAbsolute(file));
    } catch (FileNotFoundException e) {
      //nothing to do
    }
    if (fileStatus != null && !SwiftUtils.isDirectory(fileStatus)) {
      if (overwrite) {
        delete(file, true);
      } else {
        throw new SwiftException("File already exists: " + file);
      }
    } else {
      Path parent = file.getParent();
      if (parent != null) {
        if (!mkdirs(parent)) {
          throw new SwiftException("Mkdirs failed to create " + parent.toString());
        }
      }
    }

    //Here reflection is used for changing default value
    Class nativeOutputStream = SwiftNativeOutputStream.class;
    Field filePartSize;
    try {
      filePartSize = nativeOutputStream.getDeclaredField("FILE_PART_SIZE");
      filePartSize.setAccessible(true);
    } catch (NoSuchFieldException e) {
      //can be thrown after field renaming
      throw new RuntimeException("Field 'FILE_PART_SIZE' not found in class SwiftNativeOutputStream", e);
    }

    SwiftNativeOutputStream out = new SwiftNativeOutputStream(getConf(), store, file.toUri().toString());

    //set default value 1024 bytes instead 4.5Gb
    try {
      filePartSize.set(out, 1024l);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }

    return new FSDataOutputStream(out, statistics);
  }

}
