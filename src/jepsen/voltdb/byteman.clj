(ns jepsen.voltdb.byteman
  "Byteman (https://byteman.jboss.org/) support: attaches the Byteman agent to
  every VoltDB server JVM so rules can inject delays, exceptions and forced
  thread interleavings at exact points in the host layer (ENG-30396).

  The agent and submit jars are dependencies in project.clj, so their version
  is pinned in this repo. They travel with the test like the VoltDB kit does:
  setup uploads them from the control node into the node's VoltDB base
  directory, and teardown removes them with everything else there. Nothing has
  to be installed on the node images, and the same code works on the lab's LXD
  nodes and on any other SSH node.

  The agent's listener binds to localhost on the node, so rules are loaded and
  unloaded by running Byteman's submit client on the node itself, over SSH.

  This namespace depends only on jepsen.control, not on jepsen.voltdb, so the
  database code can require it."
  (:require [jepsen.control :as c]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]))

(def port
  "Port the Byteman agent listener binds to on each node (localhost only).
  9091 is Byteman's default."
  9091)

(defn dir
  "Directory on the node that holds the Byteman jars and rule files."
  [base-dir]
  (str base-dir "/byteman"))

(defn agent-jar  [base-dir] (str (dir base-dir) "/byteman.jar"))
(defn submit-jar [base-dir] (str (dir base-dir) "/byteman-submit.jar"))
(defn rules-dir  [base-dir] (str (dir base-dir) "/rules"))

(defn- local-jar
  "The jar on the control node's classpath that holds the given class file,
  found from the class resource so the class is never loaded or initialized
  in the control JVM."
  [class-resource]
  (let [url (or (io/resource class-resource)
                (throw (IllegalStateException.
                         (str class-resource " is not on the classpath; is "
                              "the Byteman dependency in project.clj?"))))
        [_ path] (re-find #"^jar:file:(.+?\.jar)!" (str url))]
    (when-not path
      (throw (IllegalStateException.
               (str class-resource " was not loaded from a jar: " url))))
    (io/file path)))

(defn local-agent-jar []
  (local-jar "org/jboss/byteman/agent/Main.class"))

(defn local-submit-jar []
  (local-jar "org/jboss/byteman/agent/submit/Submit.class"))

(def local-rules-dir
  "Rule files (*.btm) in this repo, relative to the working directory, like
  the stored procedures in procedures/."
  "resources/byteman")

(defn local-rule-files
  "The *.btm rule files in this repo, sorted by name."
  []
  (->> (io/file local-rules-dir)
       .listFiles
       (filter #(str/ends-with? (.getName %) ".btm"))
       (sort-by #(.getName %))))

(defn install!
  "Uploads the agent jar, the submit jar and every rule file to the node.
  Run after the VoltDB kit is unpacked, since that replaces base-dir."
  [base-dir]
  (c/exec :mkdir :-p (rules-dir base-dir))
  (c/upload (.getCanonicalPath (local-agent-jar)) (agent-jar base-dir))
  (c/upload (.getCanonicalPath (local-submit-jar)) (submit-jar base-dir))
  (doseq [f (local-rule-files)]
    (c/upload (.getCanonicalPath f) (str (rules-dir base-dir) "/" (.getName f))))
  (info "Byteman agent uploaded to" (dir base-dir)))

(defn agent-opts
  "The JVM option that attaches the agent with its listener on, for
  VOLTDB_OPTS. With no rules loaded the agent only adds the listener thread
  and class-load hooks.

  boot: puts the agent jar on the bootstrap classpath, which rules on JDK
  classes need (snapshot-write-ioexception.btm throws from
  sun.nio.ch.FileChannelImpl.write). Without it the woven JDK method cannot
  load Byteman's classes, and every call to it throws NoClassDefFoundError
  while such a rule is loaded."
  [base-dir]
  (str "-javaagent:" (agent-jar base-dir)
       "=listener:true,port:" port ",boot:" (agent-jar base-dir)))

(defn- submit!
  "Runs Byteman's submit client on the node against the local agent and
  returns its output. args are the client's own arguments."
  [base-dir java-home & args]
  (apply c/exec (str java-home "/bin/java")
         :-cp (submit-jar base-dir)
         "org.jboss.byteman.agent.submit.Submit"
         :-p port
         args))

(defn rule-path
  "Path on the node of a rule file from resources/byteman, by file name."
  [base-dir rule-file]
  (str (rules-dir base-dir) "/" rule-file))

(defn list-rules
  "Lists the rules loaded in the node's agent. Fails if the agent is not
  listening, so it also proves the agent is attached."
  [base-dir java-home]
  (submit! base-dir java-home))

(defn load-rules!
  "Loads the named rule files (names from resources/byteman) into the node's
  agent."
  [base-dir java-home & rule-files]
  (apply submit! base-dir java-home :-l
         (map (partial rule-path base-dir) rule-files)))

(defn unload-rules!
  "Unloads the named rule files from the node's agent, or every rule when
  none are named."
  [base-dir java-home & rule-files]
  (apply submit! base-dir java-home :-u
         (map (partial rule-path base-dir) rule-files)))
