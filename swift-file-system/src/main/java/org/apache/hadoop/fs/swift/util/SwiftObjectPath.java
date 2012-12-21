package org.apache.hadoop.fs.swift.util;

import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.regex.Pattern;

/**
 * Swift hierarchy mapping
 */
public class SwiftObjectPath {
  private static final Pattern PATH_PART_PATTERN = Pattern.compile(".*/AUTH_\\w*/");

  /**
   * Swift container
   */
  private final String container;

  /**
   * swift object
   */
  private final String object;

  public SwiftObjectPath(String container, String object) {
    this.container = container;
    this.object = object;
  }

  public String getContainer() {
    return container;
  }

  public String getObject() {
    return object;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SwiftObjectPath)) return false;
    final SwiftObjectPath that = (SwiftObjectPath) o;
    return this.toUriPath().equals(that.toUriPath());
  }

  @Override
  public int hashCode() {
    int result = container.hashCode();
    result = 31 * result + object.hashCode();
    return result;
  }

  public String toUriPath() {
    if (container.endsWith("/"))
      return container + object;
    else if (object.startsWith("/"))
      return container + object;
    else
      return container + "/" + object;
  }

  @Override
  public String toString() {
    return toUriPath();
  }

  public static SwiftObjectPath fromPath(URI uri, Path path) {
    final String url = path.toUri().getPath().replaceAll(PATH_PART_PATTERN.pattern(), "");

    return new SwiftObjectPath(uri.getHost(), url);
  }
}
