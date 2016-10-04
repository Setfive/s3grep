# s3grep

A tool to search similar to grep S3 buckets for particular strings or regular expressions.  This searches in parallel.  Files are streamed to the local machine, searched, and discarded meaning you will not need a massive amount of space to search a large S3 bucket.  

This utilizes maven and to package the jar simply run `mvn package`.  From there it can be run via `java -jar s3-grep-1.0-SNAPSHOT.jar s3grep.properties`

The s3grep.properties.example contains all configuration options.    More information at http://shout.setfive.com/2016/10/04/s3grep-searching-s3-files-and-buckets/

A prebuilt jar can be downloaded from the releases section of this repository: https://github.com/Setfive/s3grep/releases/download/v1.0/s3-grep-1.0-SNAPSHOT.jar
