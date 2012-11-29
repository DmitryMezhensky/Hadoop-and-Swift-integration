package org.apache.hadoop.swift.fs.entities;

import java.net.URI;

/**
 * Openstack Swift endpoint description
 */
public class Endpoint {

    /**
     * endpoint id
     */
    private String id;

    /**
     * Keystone admin URL
     */
    private URI adminURL;

    /**
     * Keystone internal URL
     */
    private URI internalURL;

    /**
     * public accessible URL
     */
    private URI publicURL;

    /**
     * Openstack region name
     */
    private String region;

    /**
     *
     * @return endpoint id
     */
    public String getId() {
        return id;
    }

    /**
     *
     * @param id endpoint id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     *
     * @return Keystone admin URL
     */
    public URI getAdminURL() {
        return adminURL;
    }

    /**
     *
     * @param adminURL Keystone admin URL
     */
    public void setAdminURL(URI adminURL) {
        this.adminURL = adminURL;
    }

    /**
     *
     * @return internal Keystone
     */
    public URI getInternalURL() {
        return internalURL;
    }

    /**
     *
     * @param internalURL Keystone internal URL
     */
    public void setInternalURL(URI internalURL) {
        this.internalURL = internalURL;
    }

    /**
     *
     * @return public accessible URL
     */
    public URI getPublicURL() {
        return publicURL;
    }

    /**
     *
     * @param publicURL public URL
     */
    public void setPublicURL(URI publicURL) {
        this.publicURL = publicURL;
    }


    /**
     *
     * @return Openstack region name
     */
    public String getRegion() {
        return region;
    }

    /**
     *
     * @param region Openstack region name
     */
    public void setRegion(String region) {
        this.region = region;
    }
}
