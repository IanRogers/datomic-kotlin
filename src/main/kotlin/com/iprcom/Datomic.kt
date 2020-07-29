package com.iprcom

import clojure.lang.Keyword
import clojure.lang.RT
import clojure.lang.Symbol
import clojure.lang.Var
import datomic.Database
import datomic.Datom


val REQUIRE: Var = RT.`var`("clojure.core", "require")
val DATOMS: Var by lazy {
    REQUIRE.invoke(Symbol.intern("datomic.api"))
    RT.`var`("datomic.api", "datoms")
}

typealias EntityID = Long
typealias Value = Any

fun keyword(name: String) : Keyword = RT.keyword(null as String?, name)

enum class Index(val kw: Any) {
    // why does Database.EAVT et al type these as "Object"? ugh :-(
    EAVT(Database.EAVT),
    AEVT(Database.AEVT),
    AVET(Database.AVET),
    VAET(Database.VAET)
}

// safely ensure the types of various clojure 'Object' values
//
fun Any.asEntityID() :EntityID {
    if (this !is EntityID) throw Exception("value should be a EntityID but it's not: $this")
    return this as EntityID
}
fun Any.asIterableDatoms() : Iterable<Datom> {
    if (this !is Iterable<*>) throw Exception("This should be a Iterable<Datom> but it's not: $this")
    return this as Iterable<Datom>
}

// Some utility functions to access the datomic store directly
//
fun datoms(db:Database, index:Index, v1:Value) = DATOMS.invoke(db, index.kw, v1).asIterableDatoms()
fun datoms(db:Database, index:Index, v1:Value, v2:Value) = DATOMS.invoke(db, index.kw, v1, v2).asIterableDatoms()
fun datoms(db:Database, index:Index, v1:Value, v2:Value, v3:Value) = DATOMS.invoke(db, index.kw, v1, v2, v3).asIterableDatoms()

fun getValue(db:Database, id:EntityID, attr:Any) = datoms(db, Index.EAVT, id, attr).map {it.v()}.firstOrNull()
