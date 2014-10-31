#hdfs-checksum

hdfs-checksum contains utility functions to
   * compute md5 checksum of a hdfs file. The result would be such that as if you ran md5sum on a hdfs file. (Note: Filesystem.getFileChecksum() will not give the same checksum as this, the Filesytem API call will compute md5 of md5 of CRC32 checksums

   * compute distributed (md5 of md5 of crc32 checksums) of a local file. The result would be such that as if you called Filesystem.getFileChecksum() on a local file

   * compute block level checksums for each block a hdfs file. The returned data structure is a map, something like
```clojure
      {:bytes-per-crc 512,
       :crcs-per-block 131072,
       :checksum-type "CRC32C",
       :checksums (
          {:block-id 8228927946441106746, 
           :md5 "21cd8bde61842fd239ca13e3513cc701", 
           :boundaries [0 67108864]} 
          {:block-id 7028571474334329874, 
           :md5 "29d072fe5be94218b3fec627a3c49dd7", 
           :boundaries [67108864 67108864]}
           )}
```



## Usage
* lein uberjar
* java -cp "target/*:conf" clojure.main

Make sure that the conf folder contains the hadoop cluster's configuration against which you want to work.
The configuration should atleast have the following parameters set.

* dfs.blocksize

### Repl
```clojure
user> (use 'hdfs-checksum.core)
nil

user> (import 'org.apache.hadoop.conf.Configuration)
org.apache.hadoop.conf.Configuration
user> (def conf (Configuration.))
#'user/conf


user> (doc hdfs-checksum)
-------------------------
hdfs-checksum.core/hdfs-checksum
([path conf])
  Computes the checksum of a local file in a way
   which matches how hadoop/hdfs computes
   checksums for it's files.
nil

user> (hdfs-checksum "/tmp/file" conf)
"38894e5706e4fa1acf2b125bb697cce9"


user> (doc file-checksum)
-------------------------
hdfs-checksum.core/file-checksum
([path algorithm conf])
  Computes a standard checksum of a (hdfs) file.
   The file is accessed through the hadoop
   FileSystem api
nil

user> (file-checksum "hdfs://127.0.0.1:8020/tmp/tmp_file" :MD5 conf)
"205951d1bcabb23be15e2d5c99f265bb"


user> (doc block-checksums)
-------------------------
hdfs-checksum.core/block-checksums
([path conf])
  Returns checksum per block for a hdfs file
nil

user> (block-checksums "hdfs://127.0.0.1:8020/tmp/large_file" conf)
{:bytes-per-crc 512, :crcs-per-block 131072, :checksum-type "CRC32C", :checksums ({:block-id 8228927946441106746, :md5 "21cd8bde61842fd239ca13e3513cc701", :boundaries [0 67108864]} {:block-id 7028571474334329874, :md5 "29d072fe5be94218b3fec627a3c49dd7", :boundaries [67108864 67108864]})}
user>
```
