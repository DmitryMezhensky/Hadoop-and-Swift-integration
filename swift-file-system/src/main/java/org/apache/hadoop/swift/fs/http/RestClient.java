package org.apache.hadoop.swift.fs.http;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.protocol.DefaultProtocolSocketFactory;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.hadoop.swift.fs.SwiftObjectPath;
import org.apache.hadoop.swift.fs.auth.*;
import org.apache.hadoop.swift.fs.entities.Catalog;
import org.apache.hadoop.swift.fs.entities.Endpoint;
import org.apache.hadoop.swift.fs.exceptions.SwiftConnectionException;
import org.apache.hadoop.swift.fs.exceptions.SwiftIllegalDataLocalityRequest;
import org.apache.hadoop.swift.fs.util.JSONUtil;
import org.apache.hadoop.swift.fs.util.Preconditions;
import org.jets3t.service.impl.rest.httpclient.HttpMethodReleaseInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;


public class RestClient {
    private static final String HEADER_AUTH_KEY = "X-Auth-Token";

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

    private static abstract class DeleteMethodProcessor<R> extends HttpMethodProcessor<PutMethod, R> {
        @Override
        protected final PutMethod doCreateMethod(String uri) {
            return new PutMethod(uri);
        }
    }

    private static abstract class HeadMethodProcessor<R> extends HttpMethodProcessor<HeadMethod, R> {
        @Override
        protected final HeadMethod doCreateMethod(String uri) {
            return new HeadMethod(uri);
        }
    }

    private static class LazyHolder {
        private static final RestClient INSTANCE = new RestClient();
    }

    static {
        Protocol.registerProtocol("http", new Protocol("http", new DefaultProtocolSocketFactory(), 8080));
    }

    private RestClient() {
        final Properties properties = new Properties();
        try {
            properties.load(this.getClass().getClassLoader()
                    .getResourceAsStream("swift.properties"));

            this.authUri = new URI(properties.getProperty("auth.url"));
            this.tenant = properties.getProperty("tenant");
            this.username = properties.getProperty("username");
            this.password = properties.getProperty("password");

            token = authenticate();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
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
    public InputStream getDataAsInputStream(SwiftObjectPath path, long offset, long length) {
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
    public long getContentLength(URI uri) {
        return perform(uri, new HeadMethodProcessor<Long>() {
            @Override
            public Long extractResult(HeadMethod method) throws IOException {
                if (method.getStatusCode() == HttpStatus.SC_NOT_FOUND)
                    return 0l;

                return method.getResponseContentLength();
            }
        });
    }

    public long getContentLength(SwiftObjectPath path) {
        return getContentLength(pathToURI(path, endpointURI.toString()));
    }

    /**
     * Makes HTTP GET request to Swif
     *
     * @param path           path to file
     * @param requestHeaders http headers
     * @return byte[] file data
     */
    public InputStream getDataAsInputStream(SwiftObjectPath path, final Header... requestHeaders) {

        return executeRequest(pathToURI(path, endpointURI.toString()), requestHeaders);
    }

    /**
     * Returns object location as byte[]
     *
     * @param path           path to file
     * @param requestHeaders http headers
     * @return byte[] file data
     */
    public byte[] getObjectLocation(SwiftObjectPath path, final Header... requestHeaders) {
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

    public byte[] findObjectsByPrefix(SwiftObjectPath path, final Header... requestHeaders) {
        URI uri;
        String dataLocationURI = endpointURI.toString();
        try {
            String object = path.getObject();
            if (object.startsWith("/"))
                object = object.substring(1);

            dataLocationURI = dataLocationURI.concat(path.getContainer().concat("?prefix=").concat(object));
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

    public boolean copyObject(SwiftObjectPath src, final SwiftObjectPath dst, final Header... headers) {
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
    public void upload(SwiftObjectPath path, final InputStream data, final long length, final Header... requestHeaders) {

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
    public void delete(SwiftObjectPath path, final Header... requestHeaders) {
        perform(pathToURI(path, endpointURI.toString()), new DeleteMethodProcessor<Object>() {
            @Override
            public Object extractResult(PutMethod method) throws IOException {
                return null;
            }

            @Override
            protected void setup(PutMethod method) {
                setHeaders(method, requestHeaders);
            }
        });
    }

    public Header[] headRequest(SwiftObjectPath path, final Header... requestHeaders) {
        return perform(pathToURI(path, endpointURI.toString()), new HeadMethodProcessor<Header[]>() {
            @Override
            public Header[] extractResult(HeadMethod method) throws IOException {

                return method.getResponseHeaders();
            }

            @Override
            protected void setup(HeadMethod method) {
                setHeaders(method, requestHeaders);
            }
        });
    }

    public int putRequest(SwiftObjectPath path, final Header... requestHeaders) {
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
    public AccessToken authenticate() {
        return perform(authUri, new PostMethodProcessor<AccessToken>() {
            @Override
            public AccessToken extractResult(PostMethod method) throws IOException {
                final AuthenticationResponse access =
                        JSONUtil.toObject(method.getResponseBodyAsString(), AuthenticationWrapper.class).getAccess();
                final List<Catalog> serviceCatalog = access.getServiceCatalog();
                for (Catalog catalog : serviceCatalog) {
                    if (!catalog.getName().equals("swift"))
                        continue;
                    for (Endpoint endpoint : catalog.getEndpoints()) {
                        endpointURI = endpoint.getPublicURL();
                        break;
                    }
                }
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
    private <M extends HttpMethod, R> R perform(URI uri, HttpMethodProcessor<M, R> processor) {
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(processor);

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

    private InputStream executeRequest(final URI uri, final Header... requestHeaders) {
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
    public static RestClient getInstance() {
        return LazyHolder.INSTANCE;
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
}