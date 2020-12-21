package com.iprcom

import clojure.lang.Keyword
import clojure.lang.RT
import clojure.lang.Symbol
import clojure.lang.Var
import datomic.Connection
import datomic.Database
import datomic.Datom
import datomic.Util
import java.io.Reader


val REQUIRE: Var = RT.`var`("clojure.core", "require")
val DATOMS: Var by lazy {
    REQUIRE.invoke(Symbol.intern("datomic.api"))
    RT.`var`("datomic.api", "datoms")
}

typealias EntityID = Long
typealias Value = Any
typealias QRet = Collection<List<Iterable<Any>>>


fun keyword(name: String) : Keyword {
    val kw = Util.read(name)
    if (kw !is Keyword) {
        throw Exception("name $name didn't produce a keyword $kw ($kw.javaClass.kotlin)")
    }
    return kw as Keyword
}

enum class Index(val kw: Any) {
    EAVT(Database.EAVT),
    AEVT(Database.AEVT),
    AVET(Database.AVET),
    VAET(Database.VAET)
}

val DB_ADD = keyword(":db/add")
val DB_ID = keyword(":db/id")

// safely ensure the types of various clojure 'Object' values
//
fun Any.asEntityID() : EntityID =
    if (this is EntityID) this else throw Exception("value should be a EntityID but it's not: $this")

fun Any.asIterableDatoms() : Iterable<Datom> =
    if (this is Iterable<*>) this as Iterable<Datom> else throw Exception("This should be a Iterable<Datom> but it's not: $this")


// Some utility functions to access the datomic store directly
//
fun datoms(db:Database, index:Index, v1:Value) = DATOMS.invoke(db, index.kw, v1).asIterableDatoms()
fun datoms(db:Database, index:Index, v1:Value, v2:Value) = DATOMS.invoke(db, index.kw, v1, v2).asIterableDatoms()
fun datoms(db:Database, index:Index, v1:Value, v2:Value, v3:Value) = DATOMS.invoke(db, index.kw, v1, v2, v3).asIterableDatoms()


fun getValue(db:Database, id:EntityID, attr:Keyword) = datoms(db, Index.EAVT, id, attr).firstOrNull()?.v()
fun getValues(db:Database, id:EntityID, attr:Keyword) = datoms(db, Index.EAVT, id, attr).map {it.v()}

// use AVET (rather than VAET) as it's effectively a columnar-store database
fun getEntity(db:Database, value:Value, attr:Keyword) = datoms(db, Index.AVET, attr, value).firstOrNull()?.e()?.asEntityID()
fun getEntities(db:Database, value:Value, attr:Keyword) = datoms(db, Index.AVET, attr, value).map {it.e().asEntityID()}

fun Connection.transactAll(reader: Reader) =
    Util.readAll(reader).map {
        if (it is List<*>) {
            this.transact(it).get()
        } else {
            throw Exception("non list item returned from item reader: $it")
        }
    }

fun Connection.transactAll(str: String) = this.transactAll(str.reader())