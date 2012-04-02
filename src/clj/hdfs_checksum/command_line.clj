(ns hdfs-checksum.command-line
  (:use [hdfs-checksum.core])
  (:import [java.net URI]
           [org.apache.hadoop.conf Configuration]))

(defn run [[path & args]]
  (let [scheme (.getScheme (URI. path))]
    (cond
     (= "hdfs" scheme)
     (let [algorithm (first args)]
       (println (file-checksum path algorithm (Configuration.))))
     (or (nil? scheme)
         (= "file" scheme))
     (println (hdfs-checksum path (Configuration.))))))

(run *command-line-args*)