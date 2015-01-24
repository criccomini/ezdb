### Release to Maven

To release to Sonatype maven, follow these instructions:

* https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide

Make sure the version listed in the .pom files is a -SNAPSHOT version, then run:

    mvn clean deploy release:clean release:prepare release:perform

Finally, release the published artifact.

1. Go to https://oss.sonatype.org
2. Login
3. Go to staging repositories
4. Find the plublished artifact
5. Click close
6. Click release

The Sonatype to Maven central is set to sync every couple of hours. Here's the JIRA:

* https://issues.sonatype.org/browse/OSSRH-5249

### Javadocs

To release new Javadocs:

    mvn install javadoc:aggregate -DskipTests
    cp -r target/site/javadoc/apidocs /tmp
    git checkout gh-pages
    git pull
    rm -rf ezdb-api/ target/ ezdb-leveldb/
    rm -rf javadocs/
    cp -r /tmp/apidocs/ javadocs
    git add javadocs
    git commit -am"Updating javadocs for ezdb version X"
    git push
    git checkout master
