(defproject jepsen.voltdb "0.1.0-SNAPSHOT"
  :description "Jepsen VoltDB tests"
  :url "https://github.com/jepsen-io/voltdb"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 ; 0.3.10+ needs JDK 21 on the control node (DB nodes may stay
                 ; on 17). 0.3.14 is the latest release on clojars; 0.3.15 is
                 ; still a snapshot.
                 [jepsen "0.3.14"]
                 [org.clojure/data.xml "0.0.8"]
                 ; the 16.x client declares its own netty modules, so the
                 ; netty-all pin that 13.3.2 needed is gone
                 [org.voltdb/voltdbclient "16.0.0"]
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
