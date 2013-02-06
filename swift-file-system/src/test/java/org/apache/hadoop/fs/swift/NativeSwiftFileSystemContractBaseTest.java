package org.apache.hadoop.fs.swift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Tests for NativeSwiftFS using either the in-memory Swift emulator
 * or the real client
 */
public abstract class NativeSwiftFileSystemContractBaseTest
  extends FileSystemContractBaseTest {

  private static final Log LOG = LogFactory
    .getLog(NativeSwiftFileSystemContractBaseTest.class);

  @Override
  protected void setUp() throws Exception {
    final URI uri = getFilesystemURI();
    final Configuration conf = new Configuration();

    SwiftNativeFileSystem swiftFS = createSwiftFS();
    fs = swiftFS;
    fs.initialize(uri, conf);
    super.setUp();
  }

  /**
   * Get the URI of this filesystem
   * @return a filesystem URI
   * @throws URISyntaxException Any URI parse failure
   * @throws IOException other problems
   */
  protected abstract URI getFilesystemURI()
    throws URISyntaxException, IOException;

  /**
   * Create a basic SwiftFS. This can be done differently for
   * the different implementations (memory vs. live)
   * @throws IOException
   */
  protected abstract SwiftNativeFileSystem createSwiftFS() throws IOException;

  @Override
  public void testMkdirsWithUmask() {
    //overriding to disable
  }

}
