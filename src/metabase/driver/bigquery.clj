(ns metabase.driver.bigquery
  (:require [clojure.string :as s]
            (korma [core :as k]
                   [db :as kdb])
            [korma.sql.utils :as kutils]
            (metabase [config :as config]
                      [db :as db]
                      [driver :as driver])
            [metabase.driver.generic-sql :as sql]
            [metabase.driver.generic-sql.query-processor :as sqlqp]
            (metabase.models [database :refer [Database]]
                             [table :as table])
            [metabase.util :as u]
            [metabase.util.korma-extensions :as kx])
  (:import java.util.Collections
           (com.google.api.client.googleapis.auth.oauth2 GoogleCredential GoogleCredential$Builder GoogleAuthorizationCodeFlow GoogleAuthorizationCodeFlow$Builder GoogleTokenResponse)
           com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
           com.google.api.client.http.HttpTransport
           com.google.api.client.json.JsonFactory
           com.google.api.client.json.jackson2.JacksonFactory
           (com.google.api.services.bigquery Bigquery Bigquery$Builder BigqueryScopes)
           (com.google.api.services.bigquery.model Table TableCell TableFieldSchema TableList TableList$Tables TableReference TableRow TableSchema QueryRequest QueryResponse)))

(def ^:private ^HttpTransport http-transport (GoogleNetHttpTransport/newTrustedTransport))
(def ^:private ^JsonFactory   json-factory   (JacksonFactory/getDefaultInstance))

(def ^:private ^:const ^String redirect-uri "urn:ietf:wg:oauth:2.0:oob")

(defn- ^Bigquery credential->client [^GoogleCredential credential]
  (.build (doto (Bigquery$Builder. http-transport json-factory credential)
            (.setApplicationName (str "Metabase " config/mb-version-string)))))

(defn- fetch-access-and-refresh-tokens* [^String client-id, ^String client-secret, ^String auth-code]
  {:pre  [(seq client-id) (seq client-secret) (seq auth-code)]
   :post [(seq (:access-token %)) (seq (:refresh-token %))]}
  (println (u/format-color 'magenta "Fetching BigQuery access/refresh tokens with auth-code '%s'..." auth-code)) ; TODO - log
  (let [^GoogleAuthorizationCodeFlow flow (.build (doto (GoogleAuthorizationCodeFlow$Builder. http-transport json-factory client-id client-secret (Collections/singleton BigqueryScopes/BIGQUERY))
                                                    (.setAccessType "offline")))
        ^GoogleTokenResponse response     (.execute (doto (.newTokenRequest flow auth-code)
                                                      (.setRedirectUri redirect-uri)))]
    {:access-token (.getAccessToken response), :refresh-token (.getRefreshToken response)}))

;; Memoize this function because you're only allowed to redeem an auth-code once. This way we can redeem it the first time when `can-connect?` checks to see if the DB details are
;; viable; then the second time we go to redeem it we can save the access token and refresh token with the newly created `Database` <3
(def ^:private ^{:arglists '([client-id client-secret auth-code])} fetch-access-and-refresh-tokens (memoize fetch-access-and-refresh-tokens*))

(defn- database->credential
  "Get a `GoogleCredential` for a `DatabaseInstance`."
  {:arglists '([database])}
  ^GoogleCredential [{{:keys [^String client-id, ^String client-secret, ^String auth-code, ^String access-token, ^String refresh-token], :as details} :details, id :id, :as db}]
  {:pre [(seq client-id) (seq client-secret) (or (seq auth-code)
                                                 (and (seq access-token) (seq refresh-token)))]}
  (if-not (and (seq access-token)
               (seq refresh-token))
    ;; If Database doesn't have access/refresh tokens fetch them and try again
    (let [details (-> (merge details (fetch-access-and-refresh-tokens client-id client-secret auth-code))
                      (dissoc :auth-code))]
      (when id
        (db/upd Database id :details details))
      (recur (assoc db :details details)))
    ;; Otherwise return credential as normal
    (doto (.build (doto (GoogleCredential$Builder.)
                    (.setClientSecrets client-id client-secret)
                    (.setJsonFactory json-factory)
                    (.setTransport http-transport)))
      (.setAccessToken  access-token)
      (.setRefreshToken refresh-token))))

(def ^:private ^{:arglists '([database])} ^Bigquery database->client (comp credential->client database->credential))


(defn- ^TableList list-tables
  ([{{:keys [project-id dataset-id]} :details, :as database}]
   (list-tables (database->client database) project-id dataset-id))

  ([^Bigquery client, ^String project-id, ^String dataset-id]
   {:pre [client (seq project-id) (seq dataset-id)]}
   (-> (.tables client)
       (.list project-id dataset-id)
       .execute)))

(defn- describe-database [database]
  {:pre [(map? database)]}
  {:tables (set (for [^TableList$Tables table (.getTables (list-tables database))
                      :let [^TableReference tableref (.getTableReference table)]]
                  {:name (.getTableId tableref)}))})

(defn- can-connect? [details-map]
  {:pre [(map? details-map)]}
  (boolean (describe-database {:details details-map})))


(defn- ^Table get-table
  ([{{:keys [project-id dataset-id]} :details, :as database} table-id]
   (get-table (database->client database) project-id dataset-id table-id))

  ([^Bigquery client, ^String project-id, ^String dataset-id, ^String table-id]
   {:pre [client (seq project-id) (seq dataset-id) (seq table-id)]}
   (-> (.tables client)
       (.get project-id dataset-id table-id)
       .execute)))

(def ^:private ^:const  bigquery-type->base-type
  {"BOOLEAN"   :BooleanField
   "FLOAT"     :FloatField
   "INTEGER"   :IntegerField
   "RECORD"    :DictionaryField ; RECORD -> field has a nested schema
   "STRING"    :TextField
   "TIMESTAMP" :DateTimeField})

(defn- table-schema->metabase-field-info [^TableSchema schema]
  (for [^TableFieldSchema field (.getFields schema)]
    {:name            (.getName field)
     :base-type       (bigquery-type->base-type (.getType field))
     :special-type    nil
     :preview-display true
     :pk?             false}))

(defn- describe-table
  ([table]
   (describe-table (table/database table) (:name table)))
  ([database table-name]
   {:name   table-name
    :fields (set (table-schema->metabase-field-info (.getSchema (get-table database table-name))))}))


(defn- ^QueryResponse execute-query
  ([{{:keys [project-id]} :details, :as database} query-string]
   (execute-query (database->client database) project-id query-string))

  ([^Bigquery client, ^String project-id, ^String query-string]
   {:pre [client (seq project-id) (seq query-string)]}
   (let [request (doto (QueryRequest.)
                   (.setQuery query-string))]
     (-> (.jobs client)
         (.query project-id request)
         .execute))))

(defn- process-native* [database, ^String query-string]
  (let [^QueryResponse response (execute-query database query-string)]
    (when-not (.getJobComplete response)
      (throw (Exception. (str (.getErrors response)))))
    (let [cols (table-schema->metabase-field-info (.getSchema response))]
      {:columns (map :name cols)
       :cols    cols
       :rows    (for [^TableRow row (.getRows response)]
                  (for [^TableCell cell (.getF row)]
                    (.getV cell)))})))

(defn- process-native [{^Integer database-id :database, {^String native-query :query} :native, :as ^Map query}]
  (process-native* (Database database-id) native-query))


;;; TODO

(defn- field-values-lazy-seq [field-instance]
  nil)


;;; # Generic SQL Driver Methods

(defn- date-add [unit timestamp interval]
  (k/sqlfn* :DATE_ADD timestamp interval (kx/literal unit)))

;; µs = unix timestamp in microseconds. Most BigQuery functions like strftime require timestamps in this format

(def ^:private ->µs (partial k/sqlfn* :TIMESTAMP_TO_USEC))

(defn- µs->str [format-str µs]
  (k/sqlfn* :STRFTIME_UTC_USEC µs (kx/literal format-str)))

(defn- trunc-with-format [format-str timestamp]
  (kx/->timestamp (µs->str (->µs timestamp) format-str)))

(defn- date [unit expr]
  (case unit
    :default         (kx/->timestamp expr)
    :minute          (trunc-with-format "%Y-%m-%d %H:%M:00" expr)
    :minute-of-hour  (kx/minute expr)
    :hour            (trunc-with-format "%Y-%m-%d %H:00:00" expr)
    :hour-of-day     (kx/hour expr)
    :day             (kx/->timestamp (k/sqlfn* :DATE expr))
    :day-of-week     (k/sqlfn* :DAYOFWEEK expr)
    :day-of-month    (k/sqlfn* :DAY expr)
    :day-of-year     (k/sqlfn* :DAYOFYEAR expr)
    :week            (date-add :DAY (date :day expr) (kx/- 1 (date :day-of-week expr)))
    :week-of-year    (kx/week expr)
    :month           (trunc-with-format "%Y-%m-01" expr)
    :month-of-year   (kx/month expr)
    :quarter         (date-add :MONTH
                               (trunc-with-format "%Y-01-01" expr)
                               (kx/* (kx/dec (date :quarter-of-year expr))
                                     3))
    :quarter-of-year (kx/quarter expr)
    :year            (kx/year expr)))

(defn- unix-timestamp->timestamp [expr seconds-or-milliseconds]
  (case seconds-or-milliseconds
    :seconds      (k/sqlfn* :SEC_TO_TIMESTAMP  expr)
    :milliseconds (k/sqlfn* :MSEC_TO_TIMESTAMP expr)))


;;; # Query Processing

(declare driver)

;; this is never actually connected to, just passed to korma so it applies appropriate delimiters when building SQL
(def ^:private korma-db (-> (kdb/create-db (kdb/postgres {}))
                            (update :options assoc :delimiters [\[ \]])))


(defn- entity [dataset-id table-name]
  (-> (k/create-entity (k/raw (format "[%s.%s]" dataset-id table-name)))
      (k/database korma-db)))

;; Make the dataset-id the "schema" of every field in the query because BigQuery can't figure out fields that are qualified with their just their table name
(defn- add-dataset-id-to-fields [{{{:keys [dataset-id]} :details} :database, :as query}]
  (clojure.walk/postwalk (fn [x]
                           (if (instance? metabase.driver.query_processor.interface.Field x)
                             (assoc x :schema-name dataset-id)
                             x))
                         query))

(defn- korma-form [query entity]
  (sqlqp/build-korma-form driver (add-dataset-id-to-fields query) entity))

(defn- korma-form->sql [korma-form]
  ;; replace identifiers like [shakespeare].[word] with ones like [shakespeare.word] since that's what BigQuery expects
  (s/replace (kdb/with-db korma-db
                   (k/as-sql korma-form))
             #"\]\.\[" "."))

(defn- post-process-structured [dataset-id table-name {:keys [columns rows]}]
  ;; Since we don't alias column names the come back like "veryNiceDataset_shakepeare_corpus". Strip off the dataset and table IDs
  (let [demangle-name (u/rpartial s/replace (re-pattern (str \^ dataset-id \_ table-name \_)) "")
        columns       (for [column columns]
                        (keyword (demangle-name column)))]
    (for [row rows]
      (zipmap columns row))))

(defn- process-structured [{{{:keys [dataset-id]} :details, :as database} :database, {{table-name :name} :source-table} :query, :as query}]
  {:pre [(map? database) (seq dataset-id) (seq table-name)]}
  (let [korma-form (korma-form query (entity dataset-id table-name))
        sql        (korma-form->sql korma-form)]
    (sqlqp/log-korma-form korma-form sql)
    (post-process-structured dataset-id table-name (process-native* database sql))))

(defn- prepare-value [value]
  (if (string? value)
    (str \' value \')
    value))



(defrecord BigQueryDriver []
  clojure.lang.Named
  (getName [_] "BigQuery"))

(def ^:private driver (BigQueryDriver.))

(extend BigQueryDriver
  sql/ISQLDriver
  (merge (sql/ISQLDriverDefaultsMixin)
         {:current-datetime-fn       (constantly (k/sqlfn* :CURRENT_TIMESTAMP))
          :date                      (u/drop-first-arg date)
          :field->alias              (constantly nil)
          :prepare-value             (u/drop-first-arg (comp k/raw prepare-value :value))
          :string-length-fn          (constantly :LENGTH)
          :unix-timestamp->timestamp (u/drop-first-arg unix-timestamp->timestamp)})

  driver/IDriver
  (merge driver/IDriverDefaultsMixin
         {:can-connect?          (u/drop-first-arg can-connect?)
          :describe-database     (u/drop-first-arg describe-database)
          :describe-table        (u/drop-first-arg describe-table)
          :details-fields        (constantly [{:name         "project-id"
                                               :display-name "Project ID"
                                               :placeholder  "praxis-beacon-120871"
                                               :required     true}
                                              {:name         "dataset-id"
                                               :display-name "Dataset ID"
                                               :placeholder  "toucanSightings"
                                               :required     true}
                                              {:name         "client-id"
                                               :display-name "Client ID"
                                               :placeholder  "1201327674725-y6ferb0feo1hfssr7t40o4aikqll46d4.apps.googleusercontent.com"
                                               :required     true}
                                              {:name         "client-secret"
                                               :display-name "Client Secret"
                                               :placeholder  "dJNi4utWgMzyIFo2JbnsK6Np"
                                               :required     true}
                                              {:name         "auth-code"
                                               :display-name "Auth Code"
                                               :placeholder  "4/HSk-KtxkSzTt61j5zcbee2Rmm5JHkRFbL5gD5lgkXek"
                                               :required     true}])
          :field-values-lazy-seq (u/drop-first-arg field-values-lazy-seq)
          :process-native        (u/drop-first-arg process-native)
          :process-structured    (u/drop-first-arg process-structured)}))

(driver/register-driver! :bigquery (BigQueryDriver.))
