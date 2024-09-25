(defproject jepsen.voltdb "0.1.0-SNAPSHOT"
  :description "Jepsen VoltDB tests"
  :url "https://github.com/jepsen-io/voltdb"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 ; we can't upgrade to '0.3.5' otherwise you get a java.util.SequencedCollection error, see https://aphyr.com/posts/369-classnotfoundexception-java-util-sequencedcollection
                 [jepsen "0.3.4"]
                 [org.clojure/data.xml "0.0.8"]

                 [org.voltdb/voltdbclient "13.3.2"]
                 ; VoltDB seems to depend on Netty classes but doesn't declare
                 ; a dependency on it?
                 [io.netty/netty-all "4.1.94.Final"]
                 [org.clojure/data.csv "1.0.1"]
                 ]
  :jvm-opts ["-Xmx8g"
             "-XX:MaxInlineLevel=32"
             "-server"
             "--add-opens" "java.base/java.lang=ALL-UNNAMED"
             "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED"
             "--add-opens" "java.base/java.net=ALL-UNNAMED"
             "--add-opens" "java.base/java.nio=ALL-UNNAMED"
             "--add-opens" "java.base/sun.net.www.protocol.http=ALL-UNNAMED"
             "--add-opens" "java.base/sun.net.www.protocol.https=ALL-UNNAMED"
             "--add-opens" "java.base/sun.net.www.protocol.file=ALL-UNNAMED"
             "--add-opens" "java.base/sun.net.www.protocol.ftp=ALL-UNNAMED"
             "--add-opens" "java.base/sun.net.www.protocol.jar=ALL-UNNAMED"]
  :main jepsen.voltdb.runner
  :aot  [jepsen.voltdb.runner
         clojure.tools.logging.impl])
