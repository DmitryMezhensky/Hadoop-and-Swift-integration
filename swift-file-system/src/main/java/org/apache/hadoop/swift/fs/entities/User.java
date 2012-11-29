package org.apache.hadoop.swift.fs.entities;

import org.apache.hadoop.swift.fs.auth.Roles;

import java.util.List;

/**
 * Describes user entity in Keystone
 */
public class User {

    /**
     * user id in Keystone
     */
    private String id;

    /**
     * user human readable name
     */
    private String name;

    /**
     * user roles in Keystone
     */
    private List<Roles> roles;

    /**
     * links to user roles
     */
    private List<Object> roles_links;

    /**
     * human readable username in Keystone
     */
    private String username;

    /**
     *
     * @return user id
     */
    public String getId() {
        return id;
    }

    /**
     *
     * @param id user id
     */
    public void setId(String id) {
        this.id = id;
    }


    /**
     *
     * @return user name
     */
    public String getName() {
        return name;
    }


    /**
     *
     * @param name user name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     *
      * @return user roles
     */
    public List<Roles> getRoles() {
        return roles;
    }

    /**
     *
     * @param roles sets user roles
     */
    public void setRoles(List<Roles> roles) {
        this.roles = roles;
    }

    /**
     *
     * @return user roles links
     */
    public List<Object> getRoles_links() {
        return roles_links;
    }

    /**
     *
     * @param roles_links user roles links
     */
    public void setRoles_links(List<Object> roles_links) {
        this.roles_links = roles_links;
    }

    /**
     *
     * @return username
     */
    public String getUsername() {
        return username;
    }

    /**
     *
     * @param username human readable user name
     */
    public void setUsername(String username) {
        this.username = username;
    }
}
