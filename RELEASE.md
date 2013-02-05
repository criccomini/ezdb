### Release to Maven

To release to Sonatype maven, follow these instructions:

* https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide

Then run:

    mvn clean deploy release:clean release:prepare release:perform

The Sonatype to Maven central is set to sync every couple of hours. Here's the JIRA:

* https://issues.sonatype.org/browse/OSSRH-5249

### Javadocs

To release new Javadocs:

    mvn install javadoc:javadoc
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
