package com.iprcom

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import datomic.Peer
import datomic.Peer.createDatabase
import datomic.Peer.deleteDatabase
import java.util.Date
import kotlin.system.measureTimeMillis

object IMDB {
    val url = "datomic:dev://localhost:4334/imdb"

    // we can interpolate these attr name constants into the schema for consistency
    val movie_title = keyword(":movie/title")
    val person_name = keyword(":person/name")
    val actor_movies = keyword(":actor/movies")
    val imdb_id = keyword(":imdb/id")

    // https://hashrocket.com/blog/posts/using-datomic-as-a-graph-database
    // only need to use this once when we first set up the database
    val schema = """[
 ;; movies
 {:db/id #db/id[:db.part/db]
  :db/ident $movie_title
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/fulltext true
;; :db/unique :db.unique/identity
  :db/doc "A movie's title (upsertable)"
  :db.install/_attribute :db.part/db}
  
 ;; actors
 {:db/id #db/id[:db.part/db]
  :db/ident $person_name
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/fulltext true
;; :db/unique :db.unique/identity
  :db/doc "A person's name (upsertable)"
  :db.install/_attribute :db.part/db}

 {:db/id #db/id[:db.part/db]
  :db/ident $actor_movies
  :db/valueType :db.type/ref
  :db/cardinality :db.cardinality/many
  :db/doc "An actor's ref to a movie"
  :db.install/_attribute :db.part/db}

  ;; both
  {:db/id #db/id[:db.part/db]
  :db/ident $imdb_id
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/unique :db.unique/identity
  :db/doc "The IMDB-supplied ID"
  :db.install/_attribute :db.part/db}
 ]"""

    val tsvReader = csvReader {
        delimiter = '\t'; escapeChar = '\\'; quoteChar = '\u0000'; skipMissMatchedRow = true
    }

    fun <T> timeIt(message: String = "", block: () -> T): T {
        val start = System.currentTimeMillis()
        val r = block()
        val end = System.currentTimeMillis()
        println("$message: ${end - start} ms")
        return r
    }

    // only need to do this once
    @JvmStatic
    fun main1(args: Array<String>) {
        // uncomment this to reload the database
        val deleted = deleteDatabase(url)
        println("database deleted: $deleted")

        val doneit = createDatabase(url)
        println("database created: $doneit")

        if (! doneit) {
            println("Been here before, so exiting now")
            return
        }

        val conn = Peer.connect(url)
        println("cleaning")
        conn.gcStorage(Date())
        conn.requestIndex()
        println("add schema")
        conn.transactAll(schema)
        conn.gcStorage(Date())
        conn.requestIndex()
        println("db created")
    }

    @JvmStatic
    fun main2(args: Array<String>) {
        val conn = Peer.connect(url)


//        val tmpids = mutableMapOf<String, Long>()

        println("read in all movies, documentaries etc.")
        var last_tx: Map<*, *>? = null
        var moviecount = 0
        val timemovie = measureTimeMillis {
            tsvReader.open("../imdb/title.basics.tsv") {
                last_tx = readAllWithHeaderAsSequence()
                        .map {
                            mapOf(
                                    DB_ID to it["tconst"], // used as a tmpid and gets converted to an entity id...
                                    imdb_id to it["tconst"], // ...so we also have to store the imdb_id
                                    movie_title to it["primaryTitle"]
                            )
                        }
                        .chunked(1000)
                        .map {conn.transactAsync(it)}
                        .chunked(4)
                        .map {futs ->
                            var last_tx_ret: Map<*, *>? = null
                            futs.forEach {fut ->
                                val timeElapsed = measureTimeMillis {
                                    val tx_ret = fut.get()
//                                    tmpids.putAll(tx_ret[keyword(":tempids")] as Map<String, Long>)
                                    last_tx_ret = tx_ret
                                }
                                print("$timeElapsed, ")
                            }
                            moviecount += 4000
                            println(" - $moviecount") // println(" - ${tmpids.size}")
                            last_tx_ret
                        }
                        .last()
            }
        }
        println("time to read all movies: ${timemovie / 60000}m")
        println(last_tx)

        println("cleanup and sync")
        conn.requestIndex()
        conn.gcStorage(Date())
        // conn.syncIndex(last_tx!![keyword(":foo")] as Long)
        conn.sync()

    }

    @JvmStatic
    fun main(args: Array<String>) {
        val conn = Peer.connect(url)
        // now we can grab a db value that has all the movies loaded in
        val db = conn.db()

        println("read in all actors")
        var actorcount = 0
        val timeactors = measureTimeMillis {
            tsvReader.open("../imdb/name.basics.tsv") {
                readAllWithHeaderAsSequence()
                        .map {
                            mapOf(
                                    DB_ID to it["nconst"],
                                    imdb_id to it["nconst"],
                                    person_name to it["primaryName"],
                                    actor_movies to it["knownForTitles"]!!.split(",").mapNotNull {title ->
                                        getEntity(db, title, imdb_id)
                                    }
                            )
                        }
                        .chunked(1000)
                        .map { conn.transactAsync(it) }
                        .chunked(4)
                        .forEach { futs ->
                            futs.forEach { fut ->
                                val timeElapsed = measureTimeMillis {
                                    fut.get()
                                }
                                print("$timeElapsed, ")
                            }
                            actorcount += 4000
                            println(" - $actorcount")
                        }
            }
        }
        println("time to read all actors: ${timeactors / 60000} min")

        println("All done.")
    }

    @JvmStatic
    fun main4(args: Array<String>) {
        val conn = Peer.connect(url)
        val db = conn.db()

        println("Selection of movies...")
        val titles = Peer.q(
                """[:find ?name ?title :in $ [?name ...] :where
                        [?a $person_name ?name]
                        [?a $actor_movies ?t] 
                        [?t $movie_title ?title]
                        ]""",
                db, listOf("Fred Astaire", "Lauren Bacall"))
        println(titles)

//        println("What movies do we have?")
//        val movies = Peer.q(
//                """[:find ?e ?id ?title :in $ :where
//                        [?e $movie_title ?title]
//                        [?e $imdb_id ?id]
//                        ]""",
//                db)
//        println(movies.take(5))
//
//        println("Actors?")
//        val actors = Peer.q(
//                """[:find ?e ?id ?name ?movie_id :in $ :where
//                        [?e $person_name ?name]
//                        [?e $imdb_id ?id]
//                        [?e $actor_movies ?movie_id]
//                        ]""",
//                db)
//        println(actors)

    }
}