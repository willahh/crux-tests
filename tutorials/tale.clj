(ns tale
  (:require [crux.api :as crux]
            [clojure.java.io :as io]))

(def system
  (crux/start-node
    {:crux/index-store {:kv-store {:crux/module 'crux.lmdb/->kv-store
                                   :db-dir      (io/file "data/lmdb")}}}))



(crux/submit-tx
  system
  ; tx type
  [[:crux.tx/put

    {:crux.db/id      :ids.people/Charles                   ; mandatory id for a document in Crux
     :person/name     "Charles"
     ; age 40 at 1740
     :person/born     #inst "1700-05-18"
     :person/location :ids.places/rarities-shop
     :person/str      40
     :person/int      40
     :person/dex      40
     :person/hp       40
     :person/gold     10000}

    #inst "1700-05-18"]])

(crux/submit-tx
  system
  [; rest of characters
   [:crux.tx/put
    {:crux.db/id      :ids.people/Mary
     :person/name     "Mary"
     ; age  30
     :person/born     #inst "1710-05-18"
     :person/location :ids.places/carribean
     :person/str      40
     :person/int      50
     :person/dex      50
     :person/hp       50}
    #inst "1710-05-18"]
   [:crux.tx/put
    {:crux.db/id      :ids.people/Joe
     :person/name     "Joe"
     ; age  25
     :person/born     #inst "1715-05-18"
     :person/location :ids.places/city
     :person/str      39
     :person/int      40
     :person/dex      60
     :person/hp       60
     :person/gold     70}
    #inst "1715-05-18"]])

(crux/submit-tx
  system
  [; artefacts
   ; In our tale there is a Cozy Mug...
   [:crux.tx/put
    {:crux.db/id         :ids.artefacts/cozy-mug
     :artefact/title     "A Rather Cozy Mug"
     :artefact.perks/int 3}
    #inst "1625-05-18"]

   ; ...some regular magic beans...
   [:crux.tx/put
    {:crux.db/id         :ids.artefacts/forbidden-beans
     :artefact/title     "Magic beans"
     :artefact.perks/int 30
     :artefact.perks/hp  -20}

    #inst "1500-05-18"]
   ; ...a used pirate sword...
   [:crux.tx/put
    {:crux.db/id     :ids.artefacts/pirate-sword
     :artefact/title "A used sword"}
    #inst "1710-05-18"]
   ; ...a flintlock pistol...
   [:crux.tx/put
    {:crux.db/id     :ids.artefacts/flintlock-pistol
     :artefact/title "Flintlock pistol"}
    #inst "1710-05-18"]
   ; ...a mysterious key...
   [:crux.tx/put
    {:crux.db/id     :ids.artefacts/unknown-key
     :artefact/title "Key from an unknown door"}
    #inst "1700-05-18"]
   ; ...and a personal computing device from the wrong century.
   [:crux.tx/put
    {:crux.db/id     :ids.artefacts/laptop
     :artefact/title "A Tell DPS Laptop (what?)"}
    #inst "2016-05-18"]])

(crux/submit-tx
  system
  [[:crux.tx/put
    {:crux.db/id  :ids.places/continent
     :place/title "Ah The Continent"}
    #inst "1000-01-01"]
   [:crux.tx/put
    {:crux.db/id     :ids.places/carribean
     :place/title    "Ah The Good Ol Carribean Sea"
     :place/location :ids.places/carribean}
    #inst "1000-01-01"]
   [:crux.tx/put
    {:crux.db/id     :ids.places/coconut-island
     :place/title    "Coconut Island"
     :place/location :ids.places/carribean}
    #inst "1000-01-01"]])


;; ------

(def db (crux/db system))

(crux/entity db :ids.people/Charles)

;; Datalog syntax : query ids
(crux/q db
        '[:find ?entity-id                                  ; datalog's find is like SELECT in SQL
          :where
          ; datalog's where is quite different though
          ; datalog's where block combines binding of fields you want with filtering expressions
          ; where-expressions are organised in triplets / quadruplets
          [?entity-id :person/name "Charles"]])

;; Query more fields
(crux/q db
        '[:find ?e ?name ?int
          :where
          [?e :person/name "Charles"]
          [?e :person/name ?name]
          [?e :person/int ?int]])

;; See all artefact names
(crux/q db
        '[:find ?name
          :where [_ :artefact/title ?name]])

;; Undoing the Oopsies : Delete and Evict
(crux/submit-tx
  system
  [[:crux.tx/delete :ids.artefacts/forbidden-beans
    #inst "1690-05-18"]])

(crux/submit-tx
  system
  [[:crux.tx/evict :ids.artefacts/laptop]])

(crux/q (crux/db system)
        '[:find ?name
          :where
          [_ :artefact/title ?name]])

;; Historians will know about the beans though
(def world-in-1599 (crux/db system #inst "1599-01-01"))

(crux/q world-in-1599 '[:find ?name
                        :where [_ :artefact/title ?name]])

;; Plot Development : DB References
(defn first-ownership-tx []
  [; Charles was 25 when he found the Cozy Mug
   (let [charles (crux/entity (crux/db system #inst "1725-05-17") :ids.people/Charles)]
     [:crux.tx/put
      (update charles
              ; Crux is schemaless, so we can use :person/has however we like
              :person/has
              (comp set conj)
              ; ...such as storing a set of references to other entity ids
              :ids.artefacts/cozy-mug
              :ids.artefacts/unknown-key)
      #inst "1725-05-18"])
   ; And Mary has owned the pirate sword and flintlock pistol for a long time
   (let [mary (crux/entity (crux/db system #inst "1715-05-17") :ids.people/Mary)]
     [:crux.tx/put
      (update mary
              :person/has
              (comp set conj)
              :ids.artefacts/pirate-sword
              :ids.artefacts/flintlock-pistol)
      #inst "1715-05-18"])])

(def first-ownership-tx-response
  (crux/submit-tx system (first-ownership-tx)))

(do first-ownership-tx-response)

;; Who Has What : Basic Joins
(def who-has-what-query
  '[:find ?name ?atitle
    :where
    [?p :person/name ?name]
    [?p :person/has ?artefact-id]
    [?artefact-id :artefact/title ?atitle]])

(crux/q (crux/db system #inst "1726-05-01") who-has-what-query)
; yields
;#{["Mary" "A used sword"]
;  ["Mary" "Flintlock pistol"]
;  ["Charles" "A Rather Cozy Mug"]
;  ["Charles" "Key from an unknown door"]}

(crux/q (crux/db system #inst "1716-05-01") who-has-what-query)
; yields
; #{["Mary" "A used sword"] ["Mary" "Flintlock pistol"]}

;; Parametrized Queries
;; ... If you wondered how to write conditions analogouos to SQL WHERE id IN (…​) it’s this way.
(def parametrized-query
  '[:find ?name
    :args {ids #{:ids.people/Charles :ids.people/Mary}}
    :where
    [?e :person/name ?name]
    [(contains? ids ?e)]
    :limit 10])

(crux/q (crux/db system #inst "1726-05-01") parametrized-query)
; yields
; #{["Mary"] ["Charles"]}

;; A few convenience functions
(defn entity-update
  [entity-id new-attrs valid-time]
  (let [entity-prev-value (crux/entity (crux/db system) entity-id)]
    (crux/submit-tx system
                    [[:crux.tx/put
                      (merge entity-prev-value new-attrs)
                      valid-time]])))

(defn q
  [query]
  (crux/q (crux/db system) query))

(defn entity
  [entity-id]
  (crux/entity (crux/db system) entity-id))

(defn entity-at
  [entity-id valid-time]
  (crux/entity (crux/db system valid-time) entity-id))

(defn entity-with-adjacent
  [entity-id keys-to-pull]
  (let [db (crux/db system)
        ids->entities
        (fn [ids]
          (cond-> (map #(crux/entity db %) ids)
                  (set? ids) set
                  (vector? ids) vec))]
    (reduce
      (fn [e adj-k]
        (let [v (get e adj-k)]
          (assoc e adj-k
                   (cond
                     (keyword? v) (crux/entity db v)
                     (or (set? v)
                         (vector? v)) (ids->entities v)
                     :else v))))
      (crux/entity db entity-id)
      keys-to-pull)))

; Charles became more studious as he entered his thirties
(entity-update :ids.people/Charles
               {:person/int 50}
               #inst "1730-05-18")

; Check our update
(entity :ids.people/Charles)

; Pull out everything we know about Charles and the items he has
(entity-with-adjacent :ids.people/Charles [:person/has])


;; Some tests
(def parametrized-query
  )

(crux/q (crux/db system)
        '[:find ?name ?atitle
          :where
          [?p :person/name ?name]
          [?p :person/has ?artefact-id]
          [?artefact-id :artefact/title ?atitle]])

