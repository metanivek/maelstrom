(ns maelstrom.db
  "Shared functionality for starting database 'nodes'"
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [core :as jepsen]
                    [db :as db]
                    [store :as store]]
            [maelstrom [client :as client]
                       [net :as net]
                       [process :as process]
                       [service :as service]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn init-node!
  "Sends an init message to a freshly-started node, blocking until it
  acknowledges. Throws if the node fails to initialize."
  [net test node-id]
  (let [client (client/open! net)]
    (try+
      (let [res (client/rpc!
                  client
                  node-id
                  {:type "init"
                   :node_id node-id
                   :node_ids (:nodes test)}
                  10000)]
        (when (not= "init_ok" (:type res))
          (throw+ {:type      :init-failed
                   :node      node-id
                   :response  res}
                  nil
                  (str "Expected an init_ok message, but node responded with "
                       (pr-str res)))))
      (catch [:type :maelstrom.client/timeout] e
        (throw+ {:type :init-failed
                 :node node-id}
                (:throwable &throw-context)
                (str "Expected node " node-id
                     " to respond to an init message, but node did not respond.")))
      (finally
        (client/close! client)))))

(defn start-node!
  "Starts a node's process (unless it is already running), registers it in the
  `processes` atom, and initializes it. `append?` controls whether the node's
  log file is appended to (on restart) or truncated (on first start). Safe to
  call on an already-running node, in which case it does nothing."
  [opts net processes test node-id append?]
  (when-not (get @processes node-id)
    (info "Setting up" node-id)
    ; On a restart, snapshot whatever queued up for the node while it was down.
    ; `start-node!` below installs a fresh queue, so init is delivered first;
    ; we redeliver the backlog once the node is up. On initial setup the node
    ; has no queue yet and this is nil.
    (let [backlog (net/drain-queue! net node-id)]
      (swap! processes assoc node-id
             (process/start-node!
               {:node-id  node-id
                :bin      (:bin opts)
                :args     (:args opts)
                :net      net
                :dir      (System/getProperty "java.io.tmpdir")
                :log-stderr? (:log-stderr test)
                :append?  append?
                :log-file (->> (str node-id ".log")
                               (store/path test "node-logs")
                               .getCanonicalPath)}))
      (net/up! net node-id)
      (init-node! net test node-id)
      ; The restarted node now receives the messages peers sent to the previous
      ; instance, as it would if it came back up on the same address.
      (when (seq backlog)
        (info "Redelivering" (count backlog) "buffered message(s) to" node-id)
        (net/requeue! net node-id backlog)))))

(defn db
  "Options:

      :bin - a binary to run
      :args - args to that binary
      :net - a network"
  [opts]
  (let [net       (:net opts)
        services  (atom nil)
        processes (atom {})]
    (reify db/DB
      (setup! [_ test node-id]
        ; Spawn built-in Maelstrom services
        (when (= (jepsen/primary test) node-id)
          (reset! services (service/start-services!
                             net
                             (service/default-services test))))

        ; Start and initialize this node
        (start-node! opts net processes test node-id false))

      (teardown! [_ test node]
        ; Tear down node
        (when-let [p (get @processes node)]
          (info "Tearing down" node)
          (process/stop-node! p)
          (swap! processes dissoc node))

        ; Tear down services
        (when (= node (jepsen/primary test))
          (when-let [s @services]
            (service/stop-services! s)
            (reset! services nil))))

      ; Kill/start and pause/resume return short, serializable status keywords:
      ; the nemesis records them in the history, so they must not leak process
      ; maps (which hold unserializable objects like java.lang.Process).
      db/Kill
      (kill! [_ test node]
        (if-let [p (get @processes node)]
          (do (info "Killing" node)
              (net/down! net node)
              (process/kill-node! p)
              (swap! processes dissoc node)
              :killed)
          :not-running))

      (start! [_ test node]
        ; Restart a previously-killed node, appending to its existing log.
        (if (get @processes node)
          :already-running
          (do (start-node! opts net processes test node true)
              :started)))

      db/Pause
      (pause! [_ test node]
        (if-let [p (get @processes node)]
          (do (info "Pausing" node)
              (net/down! net node)
              (process/pause-node! p)
              :paused)
          :not-running))

      (resume! [_ test node]
        (if-let [p (get @processes node)]
          (do (info "Resuming" node)
              (process/resume-node! p)
              (net/up! net node)
              :resumed)
          :not-running)))))
