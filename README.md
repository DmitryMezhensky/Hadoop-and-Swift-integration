Java library for [Hadoop](http://hadoop.apache.org/) and [Swift](http://docs.openstack.org/developer/swift/) integration. 

## Documentation
Documentation and tutorial can be find on project [Wiki](https://github.com/DmitryMezhensky/Hadoop-and-Swift-integration/wiki)


## License
The use and distribution terms for this software are covered by Apache License 2.0

## References
Reference to Hadoop issue [HADOOP-8545](https://issues.apache.org/jira/browse/HADOOP-8545)

## Abstract
This patch enables support of OpenStack Swift Object Storage. 
Swift storage hierarchy is __account -> container -> object__. Account contains containers (Amazon S3 __buckets__ analogue), each container holds huge number of objects, which are stored as blob.

Hadoop can work with different Swift regions/installations, public or private. 

It can also work with the same object stores using multiple login details.

## Hadoop and Swift integration concepts
Containers are created by users with accounts on the Swift filestore, and hold objects.

* Objects can be zero bytes long, or they can contain data.
* Objects in the container can be up to 5GB; there is a special support for larger files than this, which merges multiple objects in to one.
* Each object is referenced by it's name. An object is named by its full name, such as this-is-an-object-name.
* You can use any characters in an object name that can be 'URL-encoded'; the maximum length of a name is 1034 characters -after URL encoding.
* Names can have / charcters in them, which are used to create the illusion of a directory structure. For example dir/dir2/name. Even though this looks like a directory, it is still just a name. There is no requirement to have any entries in the container called dir or dir/dir2
* That said. if the container has zero-byte objects that look like directory names above other objects, they can pretend to be directories. Continuing the example, a 0-byte object called dir would tell clients that it is a directory while dir/dir2 or dir/dir2/name were present. This creates an illusion of containers holding a filesystem.

Client applications talk to Swift over HTTP or HTTPS, reading, writing and deleting objects using standard HTTP operations (GET, PUT and DELETE, respectively). There is also a COPY operation, that creates a new object in the container, with a new name, containing the old data. There is no rename operation itself, objects need to be copied -then the original entry deleted.

The Swift Filesystem is eventually consistent: an operation on an object may not be immediately visible to that client, or other clients. This is a consequence of the goal of the filesystem: to span a set of machines, across multiple datacentres, in such a way that the data can still be available when many of them fail. (In contrast, the Hadoop HDFS filesystem is immediately consistent, but it does not span datacenters.)

Eventual consistency can cause surprises for client applications that expect immediate consistency: after an object is deleted or overwritten, the object may still be visible -or the old data still retrievable. The Swift Filesystem client for Apache Hadoop attempts to handle this, in conjunction with the MapReduce engine, but there may be still be occasions when eventual consistency causes suprises.

## Warnings
1. Do not share your login details with anyone, which means do not log the details, or check the XML configuration files into any revision control system to which you do not have exclusive access.
2. Similarly, no use your real account details in any documentation.
3. Do not use the public service endpoint from within an OpenStack cluster, as it will run up large bills.
4. Remember: it's not a real filesystem or hierarchical directory structure. Some operations (directory rename and delete) take time and are not atomic or isolated from other operations taking place.
5. Append is not supported.
6. Unix-style permissions are not supported. All accounts with write access to a repository have unlimited access; the same goes for those with read access.
7. In the public clouds, do not make the containers public unless you are happy with anyone reading your data, and are prepared to pay the costs of their downloads.

## Limits
* Maximum length of an object path: 1024 characters
* Maximum size of a binary object: no absolute limit. Files > 5GB are partitioned into separate files in the native filesystem, and merged during retrieval.