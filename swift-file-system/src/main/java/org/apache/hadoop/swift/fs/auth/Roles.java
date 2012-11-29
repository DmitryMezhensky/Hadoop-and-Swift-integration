package org.apache.hadoop.swift.fs.auth;

/**
 * Describes user roles in Openstack system
 */
public class Roles {
    /**
     * role name
     */
    private String name;

    /**
     *
     * @return role name
     */
    public String getName() {
        return name;
    }

    /**
     *
     * @param name role name
     */
    public void setName(String name) {
        this.name = name;
    }
}
