(ns org.dthume.data.indexed-set
  (:require [clj-tuple :refer (tuple)]
			(clojure.core [reducers :as r])
            [clojure.set]
            [clojure.pprint]
            [org.dthume.data.set :as set :refer :all])
  (:import (clojure.lang Seqable Sequential ISeq IPersistentSet ILookup
                         IPersistentStack IPersistentCollection Associative
                         Counted IHashEq)))

(defprotocol Index
  (get-data [this] "Get the index data for `this` index"))

(defprotocol IndexedSet
  (index-keys [this] "Get the keys of all the indexes in this set")
  (get-index [this k] "Get the index in `this` indexed set named by `k`.")
  (assoc-index [this k idx] "Add `idx` to `this` indexed set with key `k`.")
  (dissoc-index [this k] "Remove index named by `k` from `this` indexed set."))

(defn index-data
  [s k]
  (some-> s (get-index k) get-data))

(defn primary-index
  [x]
  (reify Index
    (get-data [_] x)))

(deftype StoppedIndexSet [primary indexes mdata]
  Object
  (equals [_ x] (.equals primary x))
  (hashCode [_] (.hashCode primary))

  IHashEq
  (hasheq [this]
    (hash primary))

  clojure.lang.IObj
  (meta [_] mdata)
  (withMeta [_ mdata]
    (StoppedIndexSet. primary indexes mdata))

  Seqable
  (seq [this] (seq primary))

  IPersistentCollection
  (cons [this value]
    (if (contains? primary value)
      this
      (StoppedIndexSet. (conj primary value) indexes mdata)))
  (empty [this]
    (if (empty? primary)
      this
      (StoppedIndexSet. (empty primary) indexes mdata)))
  (equiv [this x] (.equals this x))

  ISeq
  (first [_] (first primary))
  (more [this]
    (let [f (first primary)
          r (rest primary)]
      (StoppedIndexSet. r indexes mdata)))
  (next [this]
    (if-let [t (next primary)]
      (let [f (first primary)]
        (StoppedIndexSet. t indexes mdata))))

  Counted
  (count [_] (count primary))

  ILookup
  (valAt [_ k notfound]
    (get primary k notfound))
  (valAt [this k]
    (get primary k))

  IPersistentSet
  (disjoin [this k]
    (if (contains? primary k)
      (StoppedIndexSet. (disj primary k) indexes mdata)
      this))
  (get [this k] (.valAt this k nil))

  java.util.Set
  (contains [this x] (contains? primary x))
  (containsAll [this xs] (every? #(contains? primary %) xs))
  (isEmpty [_] (empty? primary))
  (iterator [_]
    (let [t (atom primary)]
      (reify java.util.Iterator
        (next [_] (let [f (first @t)]
                    (swap! t next)
                    f))
        (hasNext [_] (boolean (first @t))))))
  (size [this] (count this))
  (toArray [_] nil)
  (toArray [_ a] nil)
  
  IndexedSet
  (index-keys [this] (conj (keys indexes) :primary))
  (get-index [this k]
    (if (= :primary k)
      (primary-index primary)
      (get indexes k)))
  (assoc-index [this k idx]
    (throw
     (IllegalArgumentException. "Cannot add index to StoppedIndexSet")))
  (dissoc-index [this k]
    (when (= :primary k)
      (throw
       (IllegalArgumentException. "Cannot remove primary index")))
    (StoppedIndexSet. primary
      (dissoc indexes k)
      mdata))

  SetAlgebra
  (set-union [lhs rhs]
    (StoppedIndexSet. (set-union primary rhs)
                      indexes
                      mdata))
  (set-intersection [lhs rhs]
    (StoppedIndexSet. (set-intersection primary rhs)
                      indexes
                      mdata))
  (set-difference [lhs rhs]
    (StoppedIndexSet. (set-difference primary rhs)
                      indexes
                      mdata)))

(defmethod print-method StoppedIndexSet
  [^StoppedIndexSet o, ^java.io.Writer w]
  (print-method (.primary o) w))

(defmethod clojure.pprint/simple-dispatch StoppedIndexSet
  [^StoppedIndexSet o]
  (clojure.pprint/simple-dispatch (.primary o)))

(deftype TrackingSet [target base changes merged mdata]
  Object
  (equals [_ x]
    (= merged x))
  (hashCode [_] (.hashCode merged))
  
  IHashEq
  (hasheq [this]
    (hash merged))
  
  clojure.lang.IObj
  (meta [_] mdata)
  (withMeta [_ mdata]
    (TrackingSet. target base changes merged mdata))
  
  Seqable
  (seq [this] (vals merged))
  
  IPersistentCollection
  (cons [this value]
    (if (contains? merged value)
      this
      (TrackingSet.
       target base
       (update-in changes (tuple :added) (fnil conj #{}) value)
       (conj merged value) mdata)))
  (empty [this]
    (TrackingSet.
     target base
     (-> changes
         (assoc :added #{})
         (assoc :removed base))
     (empty merged)
     mdata))
  (equiv [this x] (.equals this x))
  
  ISeq
  (first [_]
    (some-> merged first (nth 1) first))
  (more [this]
    (let [f (first merged)]
      (TrackingSet.
       target base
       (if (and (some? f) (contains? base f))
         (update-in changes [:removed] (fnil conj #{}) f)
         changes)
       (rest merged)
       mdata)))
  (next [this]
    (let [r (.more this)]
      (when-not (empty? r)
        r)))
  
  Counted
  (count [_] (count merged))
  
  ILookup
  (valAt [_ k notfound]
    (get merged k notfound))
  (valAt [this k]
    (get merged k))
  
  IPersistentSet
  (disjoin [this k]
    (if (contains? merged k)
      (TrackingSet.
       target base
       (if (contains? base k)
         (update-in changes [:removed] (fnil conj #{}) k)
         changes)
       (disj merged k)
       mdata)
      this))
  (get [this k] (.valAt this k nil))
  
  java.util.Set
  (contains [this x] (contains? merged x))
  (containsAll [this xs] (every? merged xs))
  (isEmpty [_] (empty? merged))
  (iterator [_]
    (let [t (atom merged)]
      (reify java.util.Iterator
        (next [_] (let [f (first @t)]
                    (swap! t next)
                    f))
        (hasNext [_] (boolean (first @t))))))
  (size [this] (count this))
  (toArray [_] nil)
  (toArray [_ a] nil)
  
  Index
  (get-data [this] merged)

  set/SetAlgebra
  (set-union [_ rhs]
    (TrackingSet.
     target base
     (-> changes
         (update-in (tuple :added)
                    (fnil set-union #{})
                    (set-difference rhs base))
         (update-in (tuple :removed)
                    (fnil set-difference #{})
                    rhs))
     (set-union merged rhs)
     mdata))
  (set-intersection [_ rhs]
    (TrackingSet.
     target base
     (-> changes
         (update-in (tuple :removed)
                    (fnil set-union #{})
                    (set-difference base rhs))
         (update-in (tuple :added)
                    (fnil set-intersection #{})
                    (set-difference rhs base)))
     (set-intersection merged rhs)
     mdata))
  (set-difference [_ rhs]
    (TrackingSet.
     target base
     (-> changes
         (update-in (tuple :removed)
                    (fnil set-union #{})
                    (set-intersection base rhs))
         (update-in (tuple :added)
                    (fnil set-difference #{})
                    rhs))
     (set-difference merged rhs)
     mdata))
  IndexedSet
  (index-keys [this]
    (index-keys target))
  (get-index [this k]
    (get-index target k))
  (assoc-index [this k idx]
    (->> "Cannot assoc indexes while indexing is paused"
         UnsupportedOperationException.
         throw))
  (dissoc-index [this k]
    (->> "Cannot dissoc indexes while indexing is paused"
         UnsupportedOperationException.
         throw)))

(defmethod print-method TrackingSet
  [^TrackingSet o, ^java.io.Writer w]
  (print-method (.merged o) w))

(defmethod clojure.pprint/simple-dispatch TrackingSet
  [^TrackingSet o]
  (clojure.pprint/simple-dispatch (.merged o)))

(defrecord TrackedChanges [added removed])

(defn tracking-set
  ([target]
     (tracking-set target (into #{} target)))
  ([target base]
     (TrackingSet. target base
                   (TrackedChanges. #{} #{})
                   base {})))

(defn tracked-changes
  [^TrackingSet ts]
  (.changes ts))

(defn- apply-changes
  [^TrackingSet ts target]
  (let [{:keys [added removed]} (.changes ts)]
    (-> target
        (set-difference removed)
        (set-union added))))

(defn revert-changes
  [^TrackingSet ts]
  (.target ts))

(defn commit-changes
  [^TrackingSet ts]
  (->> ts .target (apply-changes ts)))

(defn- mapidx
  ([indexes f]
     (->> indexes
          (r/reduce (fn [m k idx] (assoc! m k (f idx)))
                    (transient {}))
          persistent!))
  ([indexes f a1]
     (mapidx indexes #(f % a1)))
  ([indexes f a1 a2]
     (mapidx indexes #(f % a1 a2)))
  ([indexes f a1 a2 & as]
     (mapidx indexes #(apply f % a1 a2 as))))

(deftype DefaultIndexedSet [primary indexes mdata]
  Object
  (equals [_ x] (.equals primary x))
  (hashCode [_] (.hashCode primary))

  IHashEq
  (hasheq [this]
    (hash primary))

  clojure.lang.IObj
  (meta [_] mdata)
  (withMeta [_ mdata]
    (DefaultIndexedSet. primary indexes mdata))

  Seqable
  (seq [this] (seq primary))

  IPersistentCollection
  (cons [this value]
    (if (contains? primary value)
      this
      (DefaultIndexedSet. (conj primary value)
        (mapidx indexes conj value)
        mdata)))
  (empty [this]
    (if (empty? primary)
      this
      (DefaultIndexedSet. (empty primary)
        (mapidx indexes empty)
        mdata)))
  (equiv [this x] (.equals this x))

  ISeq
  (first [_] (first primary))
  (more [this]
    (let [f (first primary)
          r (rest primary)]
      (DefaultIndexedSet. r
        (if (and (some? f) (not (empty? r)))
          (mapidx indexes disj f)
          (mapidx indexes empty))
        mdata)))
  (next [this]
    (if-let [t (next primary)]
      (let [f (first primary)]
        (DefaultIndexedSet. t
          (mapidx indexes disj f)
          mdata))))

  Counted
  (count [_] (count primary))

  ILookup
  (valAt [_ k notfound]
    (get primary k notfound))
  (valAt [this k]
    (get primary k))

  IPersistentSet
  (disjoin [this k]
    (if (contains? primary k)
      (DefaultIndexedSet. (disj primary k)
        (mapidx indexes disj k)
        mdata)
      this))
  (get [this k] (.valAt this k nil))

  java.util.Set
  (contains [this x] (contains? primary x))
  (containsAll [this xs] (every? #(contains? primary %) xs))
  (isEmpty [_] (empty? primary))
  (iterator [_]
    (let [t (atom primary)]
      (reify java.util.Iterator
        (next [_] (let [f (first @t)]
                    (swap! t next)
                    f))
        (hasNext [_] (boolean (first @t))))))
  (size [this] (count this))
  (toArray [_] nil)
  (toArray [_ a] nil)
  
  IndexedSet
  (index-keys [this] (conj (keys indexes) :primary))
  (get-index [this k]
    (if (= :primary k)
      (primary-index primary)
      (get indexes k)))
  (assoc-index [this k idx]
    (when (= :primary k)
      (throw
       (IllegalArgumentException. "Cannot replace primary index")))
    (DefaultIndexedSet. primary
      (assoc indexes k (set-union idx primary))
      mdata))
  (dissoc-index [this k]
    (when (= :primary k)
      (throw
       (IllegalArgumentException. "Cannot remove primary index")))
    (DefaultIndexedSet. primary
      (dissoc indexes k)
      mdata))

  SetAlgebra
  (set-union [lhs rhs]
    (DefaultIndexedSet. (set-union primary rhs)
      (mapidx indexes set-union rhs)
      mdata))
  (set-intersection [lhs rhs]
    (let [removed (set-difference primary rhs)]
      (DefaultIndexedSet. (set-difference primary removed)
        (mapidx indexes set-difference removed)
        mdata)))
  (set-difference [lhs rhs]
    (DefaultIndexedSet. (set-difference primary rhs)
      (mapidx indexes set-difference rhs)
      mdata)))

(defmethod print-method DefaultIndexedSet [^DefaultIndexedSet o, ^java.io.Writer w]
  (print-method (.primary o) w))

(defmethod clojure.pprint/simple-dispatch DefaultIndexedSet
  [^DefaultIndexedSet o]
  (clojure.pprint/simple-dispatch (.primary o)))

(defn indexed-set
  [& {:as conf}]
  (let [{:keys [primary]
         :or {primary #{}}} conf]
    (DefaultIndexedSet. primary {} {})))

(defn pause-indexing
  [^DefaultIndexedSet s]
  (tracking-set s (.primary s)))

(defn resume-indexing
  [s]
  (commit-changes s))

(defn stop-indexing
  [^DefaultIndexedSet s]
  (->StoppedIndexSet (.primary s) (.indexes s) (.mdata s)))

(deftype UniqueIndex [key-fn idx mdata]
  Object
  (equals [_ x]
    (= idx x))
  (hashCode [_] (.hashCode idx))
  
  IHashEq
  (hasheq [this]
    (hash idx))
  
  clojure.lang.IObj
  (meta [_] mdata)
  (withMeta [_ mdata]
    (UniqueIndex. key-fn idx mdata))
  
  Seqable
  (seq [this] (vals idx))
  
  IPersistentCollection
  (cons [this value]
    (UniqueIndex. key-fn (assoc idx (key-fn value) value) mdata))
  (empty [this] (UniqueIndex. key-fn (empty idx) mdata))
  (equiv [this x] (.equals this x))

  ILookup
  (valAt [_ k notfound]
    (get idx (key-fn k) notfound))
  (valAt [this k]
    (get idx (key-fn k)))
    
  Counted
  (count [_] (count idx))
  
  IPersistentSet
  (disjoin [this k]
    (UniqueIndex. key-fn (dissoc idx (key-fn k)) mdata))
  (get [this k] (.valAt this k nil))  
  
  Index
  (get-data [this] idx)

  SetAlgebra
  (set-union [_ rhs]
    (UniqueIndex.
     key-fn
     (->> rhs
          (r/map #(tuple (key-fn %1) %1))
          (into idx))
     mdata))
  (set-intersection [_ rhs]
    (UniqueIndex.
     key-fn
     (->> rhs
          (map key-fn)
          (select-keys idx))
     mdata))
  (set-difference [_ rhs]
    (UniqueIndex.
     key-fn
     (if (instance? clojure.lang.IEditableCollection idx)
       (->> rhs
            (r/map key-fn)
            (r/reduce dissoc! (transient idx))
            persistent!)
       (->> rhs
            (r/map key-fn)
            (r/reduce dissoc idx)))
     mdata)))

(defn unique-index
  ([key-fn]
     (unique-index key-fn {}))
  ([key-fn base]
     (UniqueIndex. key-fn base {})))

(deftype GroupedIndex [key-fn empty-group idx mdata]
  Object
  (equals [_ x]
    (= idx x))
  (hashCode [_] (.hashCode idx))
  
  IHashEq
  (hasheq [this]
    (hash idx))
  
  clojure.lang.IObj
  (meta [_] mdata)
  (withMeta [_ mdata]
    (GroupedIndex. key-fn empty-group idx mdata))
  
  Seqable
  (seq [this] (vals idx))
  
  IPersistentCollection
  (cons [this value]
    (if-some [k (key-fn value)]
      (GroupedIndex.
       key-fn empty-group
       (update-in idx (tuple (key-fn value))
                  (fnil conj empty-group) value)
       mdata)
      this))
  (empty [this]
    (GroupedIndex. key-fn empty-group (empty idx) mdata))
  (equiv [this x] (.equals this x))

  ILookup
  (valAt [_ k notfound]
    (let [kk (key-fn k)]
      (if (and kk (contains? (get idx kk #{}) k))
        k
        notfound)))
  (valAt [this k]
    (let [kk (key-fn k)]
      (when (contains? (get idx kk #{}) k)
        k)))
  
  Counted
  (count [_] (r/reduce #(+ %1 (count %3)) 0 idx))
  
  IPersistentSet
  (disjoin [this k]
    (if-some [kk (key-fn k)]
      (GroupedIndex.
       key-fn empty-group
       (let [old     (get idx kk empty-group)
             updated (disj old k)]
         (if (seq updated)
           (assoc idx kk updated)
           (dissoc idx kk)))
       mdata)
      this))
  (get [this k] (.valAt this k nil))
  
  Index
  (get-data [this] idx)

  SetAlgebra
  (set-union [_ rhs]
    (GroupedIndex.
     key-fn empty-group
     (->> rhs
          (group-by key-fn)
          (r/reduce (fn [m k v]
                      (if (nil? k) m
                          (update-in m (tuple k)
                                     (fnil set-union empty-group)
                                     v)))
                  idx))
     mdata))
  (set-intersection [_ rhs]
    (GroupedIndex.
     key-fn empty-group
     (->> rhs
          (group-by key-fn)
          (r/reduce
           (fn [m k v]
             (if (nil? k) m
                 (let [nm (update-in
                           m (tuple k)
                           (fnil set-intersection empty-group) v)]
                   (if (-> nm (get k) empty?)
                     (dissoc nm k)
                     nm))))
           idx))
     mdata))
  (set-difference [_ rhs]
    (GroupedIndex.
     key-fn empty-group
     (->> rhs
          (group-by key-fn)
          (r/reduce
           (fn [m k v]
             (if (nil? k) m
                 (let [old     (get m k empty-group)
                       updated (set-difference old v)]
                   (if (seq updated)
                     (assoc m k updated)
                     (dissoc m k)))))
           idx))
     mdata)))

(defn grouped-index
  ([key-fn]
     (grouped-index key-fn {}))
  ([key-fn base]
     (grouped-index key-fn base #{}))
  ([key-fn base empty-group]
     (GroupedIndex. key-fn empty-group base {})))

(deftype SetIndex [idx mdata]
  Object
  (equals [_ x]
    (= idx x))
  (hashCode [_] (.hashCode idx))
  
  IHashEq
  (hasheq [this]
    (hash idx))
  
  clojure.lang.IObj
  (meta [_] mdata)
  (withMeta [_ mdata]
    (SetIndex. idx mdata))

  ILookup
  (valAt [_ k notfound]
    (get idx k notfound))
  (valAt [this k]
    (get idx k))
  
  Seqable
  (seq [this] (seq idx))
  
  IPersistentCollection
  (cons [this value]
    (SetIndex. (conj idx value) mdata))
  (empty [this]
    (SetIndex. (empty idx) mdata))
  (equiv [this x] (.equals this x))
  
  Counted
  (count [_] (count idx))
  
  IPersistentSet
  (disjoin [this k]
    (SetIndex. (disj idx k) mdata))
  (get [this k] (.valAt this k nil))
  
  Index
  (get-data [this] idx)

  SetAlgebra
  (set-union [_ rhs]
    (SetIndex. (set-union idx rhs) mdata))
  (set-intersection [_ rhs]
    (SetIndex. (set-intersection idx rhs) mdata))
  (set-difference [_ rhs]
    (SetIndex. (set-difference idx rhs) mdata)))

(defn set-index
  ([]
     (set-index #{}))
  ([base]
     (SetIndex. base {})))
