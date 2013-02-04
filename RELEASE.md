### Release to Maven

To release to Sonatype maven, follow these instructions:

* https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide

Then run:

    mvn clean deploy release:clean release:prepare release:perform

The Sonatype to Maven central is set to sync every couple of hours. Here's the JIRA:

* https://issues.sonatype.org/browse/OSSRH-5249
