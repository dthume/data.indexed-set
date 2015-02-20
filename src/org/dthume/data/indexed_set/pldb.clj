(ns org.dthume.data.indexed-set.pldb
  (:require [clojure.core.reducers :as r]
            [clojure.core.logic.pldb :as pldb]
            [org.dthume.data.indexed-set :as idx-set]
            [org.dthume.data.set :as set :refer :all])
  (:import
   (clojure.lang Counted IHashEq ILookup IPersistentCollection
                 IPersistentSet IObj ISeq Seqable Sequential)))

(declare maybe-apply-rels with-db with-rels)

(deftype PldbIndex [db as-rels mdata]
  Object
  (equals [_ x]
    (boolean
     (and (instance? PldbIndex x)
          (= db (.db ^PldbIndex x)))))
  (hashCode [_] (.hashCode db))
  
  IHashEq
  (hasheq [this]
    (hash db))
  
  IObj
  (meta [_] mdata)
  (withMeta [_ mdata]
    (PldbIndex. db as-rels mdata))

  IPersistentCollection
  (cons [this value]
    (with-rels this pldb/db-fact value))
  (empty [this]
    (with-db this pldb/empty-db))
  (equiv [this x] (.equals this x))
  
  Counted
  (count [_] 0)
  
  IPersistentSet
  (disjoin [this k]
    (with-rels this pldb/db-retraction k))
  (get [this k] nil)
  
  idx-set/Index
  (get-data [this] db)

  SetAlgebra
  (set-union [this rhs]
    (maybe-apply-rels this pldb/db-fact as-rels rhs))
  (set-intersection [_ rhs]
    (throw (UnsupportedOperationException.)))
  (set-difference [this rhs]
    (maybe-apply-rels this pldb/db-retraction as-rels rhs)))

(defn- with-db
  [^PldbIndex this db]
  (PldbIndex. db (.as-rels this) (.mdata this)))

(defn- apply-rels
  [db f rels]
  (r/reduce #(apply f %1 %2) db rels))

(defn- with-rels
  [^PldbIndex this f v]
  (if-let [rels (not-empty ((.as-rels this) v))]
    (->> rels
         (apply-rels (.db this) f)
         (with-db this))
    this))

(defn- set->rels
  [as-rels rels]
  (->> rels
       (r/mapcat as-rels)
       (into [])
       not-empty))

(defn- maybe-apply-rels
  [^PldbIndex this f as-rels rel-source]
  (if-let [rels (set->rels as-rels rel-source)]
    (->> rels
         (apply-rels (.db this) f)
         (with-db this))
    this))

(defn pldb-index
  ([as-rels]
     (pldb-index as-rels pldb/empty-db))
  ([as-rels base]
     (PldbIndex. base as-rels {})))

