(ns maelstrom.net
  "A simulated, mutable unordered network, supporting randomized delivery,
  selective packet loss, and long-lasting partitions."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [core :as jepsen]
                    [net :as net]
                    [os :as os]]
            [jepsen.net.proto :as net.proto]
            [maelstrom [util :as u]]
            [maelstrom.net [message :as msg]
                           [journal :as j]]
            [slingshot.slingshot :refer [try+ throw+]]
            [schema.core :as s]
            [incanter.distributions :as dist
             :refer [Distribution
                     draw
                     exponential-distribution
                     integer-distribution]])
  (:import (java.util.concurrent PriorityBlockingQueue
                                 TimeUnit)))

; Message validation
(def NodeId
  "Node identifiers are represented as strings."
  String)

(def Message
  "Messages always have a :src, :dest, and :body. An `:id` field is optional,
  and is assigned internally."
  {:src                 NodeId
   :dest                NodeId
   :body                s/Any
   (s/optional-key :id) s/Int})

(def check-message
  "Returns schema errors on the given message, if any."
  (s/checker Message))

(defn latency-compare [a b]
  (compare (:deadline a) (:deadline b)))

(defrecord ConstantDistribution [x]
  Distribution
  (draw [this] x))

(defn constant-dist
  "A constant distribution: always x"
  [x]
  (ConstantDistribution. x))

(defrecord ScaledDistribution [d scale]
  Distribution
  (draw [this] (* scale (dist/draw d))))

(defn scale-dist
  "Scales a distribution linearly by `scale`"
  [d scale]
  (ScaledDistribution. d scale))

(defn unscale-dist
  "Unwrap a ScaledDistribution."
  [sd]
  (:d sd))

(defn latency-dist
  "Takes options:

    :mean   The mean latency
    :dist   The shape of the distribution of latencies injected

  and yields an Incanter distribution, used to generate latencies for each
  message."
  [{:keys [dist mean]}]
  (case dist
    :constant     (constant-dist mean)
    :uniform      (integer-distribution 0 (* 2 mean))
    :exponential  (exponential-distribution (/ mean))))

(defn net
  "Construct a new network. Takes a latency specification map (see
  latency-dist).

      :queues      A map of receiver node ids to PriorityQueues
      :journal     A mutable log for network messages
      :p-loss      The probability of any given message being lost
      :partitions  A map of receivers to collections of sources. If a
                   source/receiver pair exists, receiver will drop packets
                   from source.
      :latency-dist   An incanter distribution used to generate latencies
                      for messages"
  [latency log-send? log-recv?]
  (atom {:queues          {}
         ; This will be filled in by the OS adapter--we need this to manage the
         ; disk file open/close lifecycle, and because we'll need a test map
         ; with a start time.
         :journal         nil
         :log-send?       log-send?
         :log-recv?       log-recv?
         :latency-dist    (latency-dist latency)
         :p-loss          0
         :partitions      {}
         :down-nodes      #{}
         :next-client-id  -1
         :next-message-id (atom -1)}))

(defn jepsen-net
  "A Jepsen network implementation which controls this virtual network."
  [net]
  (reify net.proto/Net
    (drop! [_ test src dest]
      (swap! net update-in [:partitions dest] conj src))

    (heal! [_ test]
      (swap! net assoc :partitions {}))

    (slow! [_ test]
      (swap! net update :latency-dist scale-dist 10))

    (fast! [_ test]
      (swap! net update :latency-dist unscale-dist))

    (flaky! [_ test]
      (swap! net assoc :p-loss 0.5))))

(defn jepsen-os
  "A jepsen.os/OS used to start and stop the network."
  [net]
  (reify os/OS
    (setup! [this test node]
      (when (= node (jepsen/primary test))
        (info "Starting Maelstrom network")
        (swap! net assoc :journal (j/journal test))))

    (teardown! [this test node]
      (when (= node (jepsen/primary test))
        (when-let [j (:journal @net)]
          (info "Shutting down Maelstrom network")
          (j/close! j))))))

(defn add-node!
  "Adds a node to the network."
  [net node-id]
  (assert (string? node-id) (str "Node id " (pr-str node-id)
                                 " must be a string"))
  (swap! net assoc-in [:queues node-id]
         (PriorityBlockingQueue. 11 latency-compare))
  net)

(defn remove-node!
  "Removes a node from the network."
  [net node-id]
  (swap! net update :queues dissoc node-id)
  net)

(defn down!
  "Marks a node as down (killed or paused). While a node is down, messages
  sent to it are dropped once its queue reaches max-queue-size."
  [net node-id]
  (swap! net update :down-nodes conj node-id)
  net)

(defn up!
  "Marks a node as up again, lifting the queue bound applied while it was
  down."
  [net node-id]
  (swap! net update :down-nodes disj node-id)
  net)

(defn ^PriorityBlockingQueue queue-for
  "Returns the queue for a particular recipient node."
  [net node]
  (if-let [q (-> net deref :queues (get node))]
    q
    (throw+ {:type      ::node-not-found
             :name      :node-not-found
             :code      1
             :definite? true}
            nil
            (str "No such node in network: " (pr-str node)))))

(defn validate-msg
  "Checks to make sure a message is well-formed and deliverable on the given
  deref'ed network. Returns msg if legal, otherwise throws."
  [m net]
  (let [m (msg/validate m)
        queues (get net :queues)]
    (assert (get queues (:src m))
            (str "Invalid source for message " (pr-str m)))
    (assert (get queues (:dest m))
            (str "Invalid dest for message " (pr-str m)))
    m))

(defn ^Long latency-for
  "Computes a latency, in ms, for a given message. We want our clients to have
  effectively zero latency whenever possible--as if colocated with nodes.
  Adding latency to them tends to *hide* consistency anomalies, so we avoid it.
  Later we might want to add an option for a separate client latency
  distribution, just for latency simulation purposes?"
  [net message]
  (if (u/involves-client? message)
    0
    (long (draw (:latency-dist net)))))

(def max-queue-size
  "The maximum number of messages the network buffers for a node which is
  currently down (killed or paused). A down node isn't draining its queue, so
  once it holds this many messages, further messages destined for it are
  dropped--a runaway backlog can't exhaust the heap and crash the test itself.
  A node that recovers still receives up to this many of the messages sent
  while it was down. Queues of running nodes are unbounded: under simulated
  latency they legitimately hold (arrival rate * latency) messages."
  10)

(defn send!
  "Sends a message (either a map or Message) into the network. Message must
  contain :src and :dest keys, both node IDs. Generates an :id for the message.
  Mutates and returns the network."
  [net message]
  (let [{:keys [log-send? p-loss journal next-message-id] :as n} @net
        ; Assign a new message ID for our internal bookkeeping, and construct a
        ; Message object.
        message (-> (msg/message (swap! next-message-id inc)
                                 (:src message)
                                 (:dest message)
                                 (:body message))
                    (validate-msg n))
        deadline (-> n
                     (latency-for message)
                     (* 1000000) ; ms -> ns
                     (+ (System/nanoTime)))]

    ; Journal
    (j/log-send! journal message)

    ; Log
    (when log-send? (info :send (pr-str message)))

    ; Send
    (if (< (rand) p-loss)
      net ; whoops, lost ur packet
      (let [src  (:src message)
            dest (:dest message)
            q    (queue-for net dest)]
        ; A killed or paused node isn't draining its queue, so once it's full
        ; we drop further messages rather than buffer an unbounded backlog.
        ; Messages to running nodes are never dropped.
        (when (or (not (contains? (:down-nodes n) dest))
                  (< (.size q) max-queue-size))
          (.put q {:deadline deadline
                   :message  message}))
        net))))

(defn recv!
  "Receive a message for the given node. Returns the message, and mutates the
  network. Returns `nil` if no message available in timeout-ms milliseconds."
  [net node timeout-ms]
  ; Fetch a message
  (when-let [envelope (.poll (queue-for net node)
                             timeout-ms TimeUnit/MILLISECONDS)]
    (let [{:keys [deadline message]} envelope
          dt (/ (- deadline (System/nanoTime)) 1e6)
          {:keys [log-recv? partitions journal]} @net]

      (when-not (some #{(:src message)} (get partitions node))
        ; No partition, OK, let's go!
        (do (when (pos? dt)
              ; This message isn't due for a bit; block until it's ready
              (Thread/sleep (long dt)))

            ; Log to console
            (when log-recv? (info :recv (pr-str message)))

            ; Journal
            (j/log-recv! journal message)

            ; And deliver!
            message)))))

(defn drain-queue!
  "Removes and returns all buffered message envelopes for a node as a vector,
  or nil if the node has no queue. Used to snapshot the backlog that piled up
  for a killed node, so it can be redelivered once the node restarts."
  [net node-id]
  (when-let [^PriorityBlockingQueue q (-> @net :queues (get node-id))]
    (let [drained (java.util.ArrayList.)]
      (.drainTo q drained)
      (vec drained))))

(defn requeue!
  "Re-inserts message envelopes (as produced by [[drain-queue!]]) into a node's
  queue, capped at max-queue-size. Used to redeliver a restarted node's
  backlog once it has initialized. Mutates and returns the network."
  [net node-id envelopes]
  (let [q (queue-for net node-id)]
    (doseq [e envelopes]
      (when (< (.size q) max-queue-size)
        (.put q e))))
  net)
