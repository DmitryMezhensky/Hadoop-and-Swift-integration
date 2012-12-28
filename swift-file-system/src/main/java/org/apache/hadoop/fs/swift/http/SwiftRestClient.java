package org.apache.hadoop.fs.swift.http;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.protocol.DefaultProtocolSocketFactory;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swift.auth.*;
import org.apache.hadoop.fs.swift.auth.entities.AccessToken;
import org.apache.hadoop.fs.swift.auth.entities.Catalog;
import org.apache.hadoop.fs.swift.auth.entities.Endpoint;
import org.apache.hadoop.fs.swift.exceptions.SwiftConnectionException;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.exceptions.SwiftIllegalDataLocalityRequest;
import org.apache.hadoop.fs.swift.ssl.EasySSLProtocolSocketFactory;
import org.apache.hadoop.fs.swift.util.JSONUtil;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.jets3t.service.impl.rest.httpclient.HttpMethodReleaseInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class SwiftRestClient {
  private static final Log LOG = LogFactory.getLog(SwiftRestClient.class);
  private static final String HEADER_AUTH_KEY = "X-Auth-Token";

  public static final String SWIFT_AUTH_PROPERTY = "swift.auth.url";
  public static final String SWIFT_TENANT_PROPERTY = "swift.tenant";
  public static final String SWIFT_USERNAME_PROPERTY = "swift.username";
  public static final String SWIFT_PASSWORD_PROPERTY = "swift.password";
  public static final String SWIFT_HTTP_PROTOCOL = "swift.http.port";
  public static final String SWIFT_HTTPS_PROTOCOL = "swift.https.port";
  public static final String SWIFT_REGION_PROPERTY = "swift.region";

  /**
   * authentication endpoint
   */
  private final URI authUri;

  /**
   * objects query endpoint
   */
  private URI endpointURI;

  /**
   * object location endpoint
   */
  private URI objectLocationURI;

  /**
   * Swift region. Some OpenStack installations has more than one region.
   * In this case user can specify region with which Hadoop will be working
   */
  private final String region;

  /**
   * tenant name
   */
  private final String tenant;

  /**
   * username name
   */
  private final String username;

  /**
   * user password
   */
  private final String password;

  /**
   * token for Swift communication
   */
  private AccessToken token;

  private static abstract class HttpMethodProcessor<M extends HttpMethod, R> {
    public final M createMethod(String uri) {
      final M method = doCreateMethod(uri);
      setup(method);
      return method;
    }

    /**
     * Override it to return some result after method is executed.
     */
    public abstract R extractResult(M method) throws IOException;

    protected abstract M doCreateMethod(String uri);

    /**
     * Override it to set up method before method is executed.
     */
    protected void setup(M method) {
    }
  }

  private static abstract class GetMethodProcessor<R> extends HttpMethodProcessor<GetMethod, R> {
    @Override
    protected final GetMethod doCreateMethod(String uri) {
      return new GetMethod(uri);
    }
  }

  private static abstract class PostMethodProcessor<R> extends HttpMethodProcessor<PostMethod, R> {
    @Override
    protected final PostMethod doCreateMethod(String uri) {
      return new PostMethod(uri);
    }
  }

  private static abstract class PutMethodProcessor<R> extends HttpMethodProcessor<PutMethod, R> {
    @Override
    protected final PutMethod doCreateMethod(String uri) {
      return new PutMethod(uri);
    }
  }

  private static abstract class CopyMethodProcessor<R> extends HttpMethodProcessor<CopyMethod, R> {
    @Override
    protected final CopyMethod doCreateMethod(String uri) {
      return new CopyMethod(uri);
    }
  }

  private static abstract class DeleteMethodProcessor<R> extends HttpMethodProcessor<DeleteMethod, R> {
    @Override
    protected final DeleteMethod doCreateMethod(String uri) {
      return new DeleteMethod(uri);
    }
  }

  private static abstract class HeadMethodProcessor<R> extends HttpMethodProcessor<HeadMethod, R> {
    @Override
    protected final HeadMethod doCreateMethod(String uri) {
      return new HeadMethod(uri);
    }
  }

  private static Configuration configuration;
  private static SwiftRestClient INSTANCE;

  private SwiftRestClient(Configuration configuration) throws SwiftException {
    try {
      Protocol.registerProtocol("http", new Protocol("http", new DefaultProtocolSocketFactory(),
              configuration.getInt(SWIFT_HTTP_PROTOCOL, 8080)));
      Protocol.registerProtocol("https",
              new Protocol("https", (ProtocolSocketFactory) new EasySSLProtocolSocketFactory(),
                      configuration.getInt(SWIFT_HTTPS_PROTOCOL, 443)));

      final String stringAuthUri = configuration.get(SWIFT_AUTH_PROPERTY);
      this.tenant = configuration.get(SWIFT_TENANT_PROPERTY);
      this.username = configuration.get(SWIFT_USERNAME_PROPERTY);
      this.password = configuration.get(SWIFT_PASSWORD_PROPERTY);
      this.region = configuration.get(SWIFT_REGION_PROPERTY);

      if (stringAuthUri == null)
        throw new IllegalArgumentException("Property swift.auth.url is not set");
      if (this.tenant == null)
        throw new IllegalArgumentException("Property swift.tenant is not set");
      if (this.username == null)
        throw new IllegalArgumentException("Property swift.username is not set");
      if (this.password == null)
        throw new IllegalArgumentException("Property swift.password is not set");

      this.authUri = new URI(stringAuthUri);

      token = authenticate();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("specified swift.auth.url property was incorrect ", e);
    }

  }

  /**
   * Makes HTTP GET request to Swift
   *
   * @param path   path to object
   * @param offset offset from file beginning
   * @param length file length
   * @return byte[] file data
   */
  public InputStream getDataAsInputStream(SwiftObjectPath path, long offset, long length) throws SwiftException {
    if (offset < 0)
      throw new IllegalArgumentException("Invalid offset: " + offset + ".");
    if (length <= 0)
      throw new IllegalArgumentException("Invalid length: " + length + ".");

    final String range = String.format("bytes=%d-%d", offset, offset + length - 1);
    return getDataAsInputStream(path, new Header("Range", range));
  }

  /**
   * Returns object length
   *
   * @param uri file URI
   * @return object length
   */
  public long getContentLength(URI uri) throws SwiftException {
    return perform(uri, new HeadMethodProcessor<Long>() {
      @Override
      public Long extractResult(HeadMethod method) throws IOException {
        if (method.getStatusCode() == HttpStatus.SC_NOT_FOUND)
          return 0l;

        return method.getResponseContentLength();
      }
    });
  }

  public long getContentLength(SwiftObjectPath path) throws SwiftException {
    return getContentLength(pathToURI(path, endpointURI.toString()));
  }

  /**
   * Makes HTTP GET request to Swif
   *
   * @param path           path to file
   * @param requestHeaders http headers
   * @return byte[] file data
   */
  public InputStream getDataAsInputStream(SwiftObjectPath path, final Header... requestHeaders) throws SwiftException {

    return executeRequest(pathToURI(path, endpointURI.toString()), requestHeaders);
  }

  /**
   * Returns object location as byte[]
   *
   * @param path           path to file
   * @param requestHeaders http headers
   * @return byte[] file data
   */
  public byte[] getObjectLocation(SwiftObjectPath path, final Header... requestHeaders) throws SwiftException {
    return perform(pathToURI(path, objectLocationURI.toString()), new GetMethodProcessor<byte[]>() {
      @Override
      public byte[] extractResult(GetMethod method) throws IOException {
        if (method.getStatusCode() == HttpStatus.SC_NOT_FOUND)
          return null;

        return method.getResponseBody();
      }

      @Override
      protected void setup(GetMethod method) {
        setHeaders(method, requestHeaders);
      }
    });
  }

  public byte[] findObjectsByPrefix(SwiftObjectPath path, final Header... requestHeaders) throws SwiftException {
    URI uri;
    String dataLocationURI = endpointURI.toString();
    try {
      String object = path.getObject();
      if (object.startsWith("/"))
        object = object.substring(1);

      dataLocationURI = dataLocationURI.concat("/").concat(path.getContainer().concat("/?prefix=").concat(object));
      uri = new URI(dataLocationURI);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return perform(uri, new GetMethodProcessor<byte[]>() {
      @Override
      public byte[] extractResult(GetMethod method) throws IOException {
        if (method.getStatusCode() == HttpStatus.SC_NOT_FOUND)
          return null;

        return method.getResponseBody();
      }

      @Override
      protected void setup(GetMethod method) {
        setHeaders(method, requestHeaders);
      }
    });
  }

  public boolean copyObject(SwiftObjectPath src, final SwiftObjectPath dst, final Header... headers)
          throws SwiftException {
    return perform(pathToURI(src, endpointURI.toString()), new CopyMethodProcessor<Boolean>() {
      @Override
      public Boolean extractResult(CopyMethod method) throws IOException {
        return method.getStatusCode() != HttpStatus.SC_NOT_FOUND;
      }

      @Override
      protected void setup(CopyMethod method) {
        setHeaders(method, headers);
        method.addRequestHeader("Destination", dst.toUriPath());
      }
    });
  }

  /**
   * Uploads file as Input Stream to Swift
   *
   * @param path           path to Swift
   * @param data           object data
   * @param requestHeaders http headers
   */
  public void upload(SwiftObjectPath path, final InputStream data, final long length, final Header... requestHeaders)
          throws SwiftException {

    perform(pathToURI(path, endpointURI.toString()), new PutMethodProcessor<byte[]>() {
      @Override
      public byte[] extractResult(PutMethod method) throws IOException {
        return method.getResponseBody();
      }

      @Override
      protected void setup(PutMethod method) {
        method.setRequestEntity(new InputStreamRequestEntity(data, length));
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * Deletes object from swift
   *
   * @param path           path to file
   * @param requestHeaders http headers
   */
  public boolean delete(SwiftObjectPath path, final Header... requestHeaders) throws SwiftException {

    return perform(pathToURI(path, endpointURI.toString()), new DeleteMethodProcessor<Boolean>() {
      @Override
      public Boolean extractResult(DeleteMethod method) throws IOException {
        return method.getStatusCode() == HttpStatus.SC_NO_CONTENT;
      }

      @Override
      protected void setup(DeleteMethod method) {
        setHeaders(method, requestHeaders);
      }
    });
  }

  public Header[] headRequest(SwiftObjectPath path, final Header... requestHeaders) throws SwiftException {
    return perform(pathToURI(path, endpointURI.toString()), new HeadMethodProcessor<Header[]>() {
      @Override
      public Header[] extractResult(HeadMethod method) throws IOException {
        if (method.getStatusCode() == HttpStatus.SC_NOT_FOUND)
          return null;

        return method.getResponseHeaders();
      }

      @Override
      protected void setup(HeadMethod method) {
        setHeaders(method, requestHeaders);
      }
    });
  }

  public int putRequest(SwiftObjectPath path, final Header... requestHeaders) throws SwiftException {
    return perform(pathToURI(path, endpointURI.toString()), new PutMethodProcessor<Integer>() {

      @Override
      public Integer extractResult(PutMethod method) throws IOException {

        return method.getStatusCode();
      }

      @Override
      protected void setup(PutMethod method) {
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * Makes authentication in Openstack Keystone
   *
   * @return authenticated access token
   */
  public AccessToken authenticate() throws SwiftException {
    LOG.info("started authentication");
    return perform(authUri, new PostMethodProcessor<AccessToken>() {
      @Override
      public AccessToken extractResult(PostMethod method) throws IOException {
        final AuthenticationResponse access =
                JSONUtil.toObject(method.getResponseBodyAsString(), AuthenticationWrapper.class).getAccess();
        final List<Catalog> serviceCatalog = access.getServiceCatalog();
        for (Catalog catalog : serviceCatalog) {
          if (catalog.getName().equals("swift") || catalog.getName().equals("cloudFiles")
                  || catalog.getType().equals("object-store")) {
            for (Endpoint endpoint : catalog.getEndpoints()) {
              if (region != null && !endpoint.getRegion().equals(region)) {
                continue;
              } else {
                endpointURI = endpoint.getPublicURL();
                break;
              }
            }
          }
        }
        if (endpointURI == null)
          throw new SwiftException("SwiftRestClient not found swift endpoint".
                  concat(region == null ? "" : " with specified region: " + region));

        try {
          objectLocationURI = new URI(endpointURI.getScheme().
                  concat("://").concat(
                  endpointURI.getHost().
                          concat(":").
                          concat(String.valueOf(endpointURI.getPort())).
                          concat("/object_endpoint/AUTH_").
                          concat(access.getToken().getTenant().getId())));
        } catch (URISyntaxException e) {
          throw new RuntimeException("object endpoint URI is incorrect", e);
        }
        token = access.getToken();
        //create default container if it doesn't exist for Hadoop Swift integration
        final InputStream dataAsInputStream = getDataAsInputStream(new SwiftObjectPath(endpointURI.getHost(), ""));
        if (dataAsInputStream == null) {
          final int status = putRequest(new SwiftObjectPath(endpointURI.getHost(), ""));
          if (status != HttpStatus.SC_OK && status != HttpStatus.SC_CREATED &&
                  status != HttpStatus.SC_ACCEPTED && status != HttpStatus.SC_NO_CONTENT) {
            throw new SwiftException("Couldn't create container " + endpointURI.getHost() +
                    " for storing data in Swift. Try to create container " + endpointURI.getHost() + " manually ");
          }
        }
        LOG.info("authenticated successfully");

        return token;
      }

      @Override
      protected void setup(PostMethod method) {
        final String data = JSONUtil.toJSON(new AuthenticationRequestWrapper(
                new AuthenticationRequest(tenant, new PasswordCredentials(username, password))));

        try {
          method.setRequestEntity(new StringRequestEntity(data, "application/json", "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  /**
   * Performs request
   *
   * @param uri       URI to source
   * @param processor HttpMethodProcessor
   * @param <M>       method
   * @param <R>       result type
   * @return result of HTTP request
   */
  private <M extends HttpMethod, R> R perform(URI uri, HttpMethodProcessor<M, R> processor) throws SwiftException {
    checkNotNull(uri);
    checkNotNull(processor);

    final HttpClient client = new HttpClient();
    final M method = processor.createMethod(uri.toString());
    method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

    try {
      int statusCode = client.executeMethod(method);
      retryIfAuthorizationFailed(client, method);

      if (statusCode == HttpStatus.SC_BAD_REQUEST) {
        throw new SwiftIllegalDataLocalityRequest("Illegal path");
      }

      if ((statusCode != HttpStatus.SC_OK) && (statusCode != HttpStatus.SC_PARTIAL_CONTENT) && statusCode != 201
              && statusCode != 204 && statusCode != 202 && method.getStatusCode() != HttpStatus.SC_NOT_FOUND)
        throw new SwiftConnectionException(String.format("Method failed, status code: %d, status line: %s (uri: %s)",
                statusCode, method.getStatusLine(), uri));

      return processor.extractResult(method);
    } catch (HttpException e) {
      method.releaseConnection();
      throw new SwiftConnectionException("Fatal protocol violation: " + e.getMessage(), e);
    } catch (IOException e) {
      method.releaseConnection();
      throw new SwiftConnectionException("Fatal transport error: " + e.getMessage(), e);
    }
  }

  private InputStream executeRequest(final URI uri, final Header... requestHeaders) throws SwiftException {
    return perform(uri, new GetMethodProcessor<InputStream>() {
      @Override
      public InputStream extractResult(GetMethod method) throws IOException {
        if (method.getStatusCode() == HttpStatus.SC_NOT_FOUND)
          return null;

        return new HttpMethodReleaseInputStream(method);
      }

      @Override
      protected void setup(GetMethod method) {
        setHeaders(method, requestHeaders);
      }
    });
  }

  /**
   * @return REST client instance
   */
  public static SwiftRestClient getInstance(Configuration config) throws SwiftException {
    if (configuration == null && INSTANCE == null) {
      configuration = config;
      INSTANCE = new SwiftRestClient(config);
      return INSTANCE;
    } else {
      if (!checkConfigEquality(config))
        INSTANCE = new SwiftRestClient(config);

      return INSTANCE;
    }
  }

  private static boolean checkConfigEquality(Configuration current) {
    if (!configuration.get(SWIFT_TENANT_PROPERTY, "default-unset-property").
            equals(current.get(SWIFT_TENANT_PROPERTY, "default-unset-property")))
      return false;
    if (!configuration.get(SWIFT_AUTH_PROPERTY, "default-unset-property").
            equals(current.get(SWIFT_AUTH_PROPERTY, "default-unset-property")))
      return false;
    if (!configuration.get(SWIFT_HTTP_PROTOCOL, "default-unset-property").
            equals(current.get(SWIFT_HTTP_PROTOCOL, "default-unset-property")))
      return false;
    if (!configuration.get(SWIFT_HTTPS_PROTOCOL, "default-unset-property").
            equals(current.get(SWIFT_HTTPS_PROTOCOL, "default-unset-property")))
      return false;
    if (!configuration.get(SWIFT_USERNAME_PROPERTY, "default-unset-property").
            equals(current.get(SWIFT_USERNAME_PROPERTY, "default-unset-property")))
      return false;
    if (!configuration.get(SWIFT_PASSWORD_PROPERTY, "default-unset-property").
            equals(current.get(SWIFT_PASSWORD_PROPERTY, "default-unset-property")))
      return false;
    if (!configuration.get(SWIFT_REGION_PROPERTY, "default-unset-property").
            equals(current.get(SWIFT_REGION_PROPERTY, "default-unset-property")))
      return false;

    return true;
  }

  /**
   * Converts Swift path to URI to maek request
   *
   * @param path            path to object
   * @param dataLocationURI damain url e.g. http://domain.com
   * @return valid URI for object
   */
  private URI pathToURI(SwiftObjectPath path, String dataLocationURI) {
    URI uri;
    try {
      if (path.toString().startsWith("/"))
        dataLocationURI = dataLocationURI.concat(path.toUriPath());
      else
        dataLocationURI = dataLocationURI.concat("/").concat(path.toUriPath());

      uri = new URI(dataLocationURI);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return uri;
  }

  private void setHeaders(HttpMethodBase method, Header[] requestHeaders) {
    for (Header header : requestHeaders)
      method.addRequestHeader(header);
    method.addRequestHeader(HEADER_AUTH_KEY, token.getId());
  }

  private <M extends HttpMethod> void retryIfAuthorizationFailed(HttpClient client, M method) throws IOException {
    if (method.getStatusCode() == HttpStatus.SC_UNAUTHORIZED
            && method.getURI().toString().equals(authUri.toString()))
      throw new SwiftConnectionException("Authentication failed, credentials are incorrect,\n " +
              "or Openstack Keystone is configured incorrectly");
    if (method.getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
      authenticate();
      client.executeMethod(method);
    }
  }

  /**
   * Ensures that an object reference passed as a parameter to the calling
   * method is not null.
   *
   * @param reference an object reference
   * @return the non-null reference that was validated
   * @throws NullPointerException if {@code reference} is null
   */
  private static <T> T checkNotNull(T reference) {
    if (reference == null) {
      throw new NullPointerException();
    }
    return reference;
  }
}
