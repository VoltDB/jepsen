(ns jepsen.voltdb.lazyfs
  "Runs VoltDB's durable files on LazyFS (https://github.com/dsrhaslab/lazyfs),
  a FUSE filesystem that keeps written data in its own cache until it is
  fsynced. Dropping that cache after killing VoltDB loses exactly what a power
  loss would.

  LazyFS is mounted at <base-dir>/lazyfs, and the deployment points the
  command log, command log snapshot and snapshot paths into it (see
  `deployment-paths`). It does not wrap voltdbroot itself: LazyFS has no statfs,
  so the mount reports 0 bytes free, and VoltDB refuses to start when
  voltdbroot has under 1 MB free (FsUtils.ensureMinimalFreeSpace).

  Builds on jepsen.lazyfs, with our own install and mount: Jepsen's install
  needs the Debian 13 package libfuse3-4, and both build and mount in the fixed
  directory /opt/jepsen/lazyfs, which the lab nodes share."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [lazyfs :as lazyfs]
                    [util :refer [await-fn meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [throw+]]))

(def version
  "LazyFS release we build, and its commit. 0.3.1 adds the torn-op and
  torn-seq faults that Jepsen's default (0.3.0) lacks."
  {:tag "0.3.1", :commit "045a0b3a1126725e693934e29d3ba15e08cc39ec"})

(defn mount-dir
  "Where LazyFS is mounted on each node."
  [base-dir]
  (str base-dir "/lazyfs"))

(defn deployment-paths
  "The <paths> children that put VoltDB's durable files on LazyFS."
  [base-dir]
  (let [d (mount-dir base-dir)]
    [[:commandlog         {:path (str d "/command_log")}]
     [:commandlogsnapshot {:path (str d "/command_log_snapshot")}]
     [:snapshots          {:path (str d "/snapshots")}]]))

(def cache-size
  "LazyFS cache size. LazyFS allocates all of it up front."
  "0.5GB")

(defn config
  "LazyFS config text. Unlike jepsen.lazyfs/config this turns on LRU eviction,
  which writes dirty pages back before reusing them, as a kernel's page cache
  would. With eviction off, LazyFS keeps every page it has ever cached (an
  fsync only marks pages clean), and once the cache is full it writes straight
  through to the backing directory, so nothing written after that can be lost:
  the test would pass while testing nothing."
  [{:keys [fifo log-file]}]
  (str "[faults]\n"
       "fifo_path=\"" fifo "\"\n"
       "[cache]\n"
       "apply_eviction=true\n"
       "[cache.simple]\n"
       "custom_size=\"" cache-size "\"\n"
       "blocks_per_page=1\n"
       "[filesystem]\n"
       "logfile=\"" log-file "\"\n"
       "log_all_operations=false\n"))

(defn lazyfs-map
  "jepsen.lazyfs's description of our mount: its backing directory, fifo,
  config and log all live in <base-dir>/lazyfs.lazyfs."
  [base-dir]
  (lazyfs/lazyfs {:dir (mount-dir base-dir)}))

(defn install-dir
  "Where LazyFS is built. Per host, since /opt is shared between lab nodes,
  and per commit, so a build is reused by later runs."
  []
  (str "/opt/jepsen-lazyfs/" (c/exec :hostname) "/" (:commit version)))

(defn install!
  "Builds the pinned LazyFS on this node, unless that build is already there.
  Returns the install directory."
  []
  (c/su
    (let [dir (install-dir)
          bin (str dir "/lazyfs/build/lazyfs")]
      (debian/install [:g++ :cmake :libfuse3-dev :fuse3 :git])
      ; From jepsen.lazyfs: LXC containers can lose /dev/fuse on reboot
      (when-not (cu/exists? lazyfs/fuse-dev)
        (c/exec :mknod lazyfs/fuse-dev "c" 10 229)
        (c/exec :chmod "a+rw" lazyfs/fuse-dev))
      (c/exec :sed :-i "/\\s*user_allow_other/s/^#//g" "/etc/fuse.conf")
      (if (cu/exists? bin)
        (info "LazyFS" (:tag version) "already built in" dir)
        (do (info "Building LazyFS" (:tag version) "in" dir)
            (c/exec :rm :-rf dir)
            (c/exec :mkdir :-p dir)
            (c/exec :git :clone :-q lazyfs/repo-url dir)
            (c/cd dir
                  (c/exec :git :checkout :-q (:commit version))
                  (c/cd "libs/libpcache" (c/exec "./build.sh"))
                  (c/cd "lazyfs" (c/exec "./build.sh")))))
      dir)))

(defn mount!
  "Installs LazyFS and mounts it at (mount-dir base-dir), empty. Call after
  the kit is unpacked, which replaces base-dir."
  [base-dir]
  (let [install (install!)
        {:keys [dir data-dir config-file log-file] :as m} (lazyfs-map base-dir)]
    (c/su
      (info "Mounting LazyFS at" dir)
      (c/exec :mkdir :-p dir data-dir)
      (c/exec :touch log-file)
      (cu/write-file! (config m) config-file)
      ; The mount script must run from the lazyfs directory
      (c/cd (str install "/lazyfs")
            (c/exec "scripts/mount-lazyfs.sh"
                    :-c config-file :-m dir :-r data-dir))
      (await-fn (fn check-mounted []
                  (or (re-find #"lazyfs" (c/exec :findmnt dir))
                      (throw+ {:type ::not-mounted, :dir dir})))
                {:retry-interval 500
                 :log-interval   5000
                 :log-message    "Waiting for LazyFS to mount"}))
    m))

(defn umount!
  "Unmounts LazyFS, if mounted, and removes its backing directory. Safe to
  call when nothing is mounted, e.g. before setup."
  [base-dir]
  (meh (lazyfs/umount! (lazyfs-map base-dir))))

(defn report!
  "Asks LazyFS to write its cache usage and the files holding unsynced data to
  its log, if it is mounted."
  [base-dir]
  (let [m (lazyfs-map base-dir)]
    (when (lazyfs/mounted? m)
      (meh (lazyfs/fifo! m "lazyfs::display-cache-usage"))
      (meh (lazyfs/fifo! m "lazyfs::unsynced-data-report"))
      ; LazyFS reads the fifo on its own thread; let it log before we copy
      (Thread/sleep 1000))))

(defn log-files
  "LazyFS's own log, for the test's log files. Reports cache usage into it
  first."
  [base-dir]
  (report! base-dir)
  [(:log-file (lazyfs-map base-dir))])
