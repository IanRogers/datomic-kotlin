package com.iprcom

import datomic.Database
import datomic.Peer

val TRACK_ARTISTS = keyword(":track/artists")
val ARTIST_NAME = keyword(":artist/name")
val TRACK_NAME = keyword(":track/name")

object MBrainz {

    // Given an artist ID what are the IDs of the tracks they were involved with
    fun artists(db: Database, id: Any) = datoms(db, Index.EAVT, id, TRACK_ARTISTS).map {it.v().asEntityID()}
    // Given a track ID what are the IDs of the artists on that track
    fun tracks(db: Database, id: Any) = datoms(db, Index.VAET, id, TRACK_ARTISTS).map {it.e().asEntityID()}
    // if id is an artist then get their tracks; if it's a track then get the artists
    fun at_neighbors(db: Database, id: EntityID) = artists(db, id).ifEmpty { tracks(db, id) }
    // What is then name value of an item (track or artist)
    fun item_name(db: Database, id: EntityID) = getValue(db, id, ARTIST_NAME) ?: getValue(db, id, TRACK_NAME)

    @JvmStatic
    fun datalog_sp(db: Database, start: EntityID, finish: EntityID) : Iterable<Any>?
            = shortestPath(start, finish, {at_neighbors(db, it)})

    @JvmStatic
    fun main(args: Array<String>) {
        val conn = Peer.connect("datomic:dev://localhost:4334/mbrainz-1968-1973")
        val db = conn.db()

        fun artist_id(name: String) =
            Peer.q("[:find ?e :in $ ?name :where [?e :artist/name ?name]]", db, name).first().first().asEntityID()
        val george_harrison = artist_id("George Harrison")
        val yvette_mimieux = artist_id("Yvette Mimieux")

        println("George Harrison: $george_harrison")
        println("Yvette Mimieux: $yvette_mimieux")

        val path = shortestPath(george_harrison, yvette_mimieux, {at_neighbors(db, it)})
        println(path)
        println(path!!.map {item_name(db, it)})

        println("put the shortest path and the top'n'tail into a single query")
        val ret = Peer.q(
                """[:find ?path :in $ ?start ?finish :where 
                        [?se :artist/name ?start]
                        [?sf :artist/name ?finish]
                        [(com.iprcom.MBrainz/datalog_sp $ ?se ?sf) ?path]
                        ]""",
                db, "George Harrison", "Yvette Mimieux")
        println(ret)
        // clojure API swallows the type of everything :-(
        val path2 = ret as Iterable<Iterable<Iterable<Any>>>
        println(path2.first().first().map {item_name(db, it as EntityID)})

        println("Try Yoko Ono to Yvette Mimieux")
        val yo = artist_id("Yoko Ono")
        println("Yoko Ono: " + yo)
        println(Peer.q(
                """[:find ?path :in $ ?start ?finish :where 
                        [?se :artist/name ?start]
                        [?sf :artist/name ?finish]
                        [(com.iprcom.MBrainz/datalog_sp $ ?se ?sf) ?path]
                        ]""",
                db, "Yoko Ono", "Yvette Mimieux"))
        println("The result [] means that the datalog query failed, i.e. there's no path")
    }

}