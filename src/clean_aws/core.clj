(ns clean-aws.core
  (require [aws.sdk.s3 :as s3]))

(def valid-buckets ["gotham-comcast-spotlight" "gotham-cox"])

(defn get-objects [cred bucket env & {next-marker :next-marker}]
  "Get the next set of objects from s3.
   See http://weavejester.github.io/clj-aws-s3/aws.sdk.s3.html#var-list-objects"
  (println (str "Getting objects for " bucket " - Next marker: " next-marker))
  (let [max-keys 100
        prefix (str env "/take_screenshot")]
    (s3/list-objects cred bucket {:max-keys max-keys
                                  :prefix prefix
                                  :marker next-marker})))

(defn move-object [cred bucket key new-key]
  "Take a key string, and a new-key to move it to"
  (println (str key " -> " new-key))
  ;(s3/copy-object cred bucket key new-key)
  ;(s3/delete-object cred bucket key)
  )

(defn move-objects [cred bucket env objects]
  "
  Take a bucket name, and the list of objects, and move to old dir. Note that
  just throwing 'future' puts it on a thread, and we return the 'future' (which
  is very similar to a JS promise).
  "
  (future
    (println (count objects))
    (doseq [o objects]
      (let [key (:key o)
            new-key (clojure.string/replace key env (str env "/old"))]
        (move-object cred bucket key new-key)))))

(defn dump-object-names [objects]
  "Take a list of objects, and just print the names of the files"
  (doseq [o objects]
    (let [key (:key o)]
      (println key))))

(defn clean-bucket [cred bucket env]
  "Loop until there are no more objects in the bucket, moving them as we go"
  (println bucket)

  ; Start our loop off with these values. The values will be replaced in the
  ; loop by the recur call below
  (loop [iteration 0
         next-marker nil
         object-data (get-objects cred bucket env)]

    (println "")
    (println (str "Iteration " iteration ", next: " next-marker))

    ; This is a failsafe to make sure i don't have an infinite loop
    (if (> iteration 5)

      (println (str "Leaving loop - iteration: " iteration))

      (do

        ;(dump-object-names (:objects object-data))
        (move-objects cred bucket env (:objects object-data))

        ; if the list is truncated (aka it has more objects in the list), go
        ; back for more
        (if (:truncated? object-data)

          ; loop it back again, with new values for the loop args. This is where
          ; we get the next set of objects (by using the next-marker from the object-data)
          (let [next-it (inc iteration)
                next-marker (:next-marker object-data)
                next-obj (get-objects cred bucket env :next-marker next-marker)]
            (recur next-it next-marker next-obj))

          (println "End of the line for these objects"))))))
