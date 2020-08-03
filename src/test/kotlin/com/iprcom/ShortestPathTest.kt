package com.iprcom

import io.kotest.core.spec.style.AnnotationSpec
import io.kotest.matchers.collections.shouldBeIn
import io.kotest.matchers.shouldBe

class ShortestPathTest : AnnotationSpec() {

    @Test
    fun test1() {
        val graph = mapOf( // undirected links, so prev and succ are the same
                "a" to listOf("b"),
                "b" to listOf("a", "c", "d"),
                "c" to listOf("b", "d", "e"),
                "d" to listOf("b", "c", "e"),
                "e" to listOf("c", "d", "f"),
                "f" to listOf("e")
        )

        shortestPath("a", "e", {graph[it]!!}, {graph[it]!!})!!.toList() shouldBeIn listOf(
                listOf("a", "b", "c", "e"), listOf("a", "b", "d", "e")
        )
    }

    @Test
    fun test2() {
        val graph = mapOf( // directed graph
                "a" to mapOf("out" to listOf("b"), "in" to listOf()),
                "b" to mapOf("out" to listOf("c"), "in" to listOf("a", "d")),
                "c" to mapOf("out" to listOf("e"), "in" to listOf("b", "d")),
                "d" to mapOf("out" to listOf("b", "c"), "in" to listOf("e")),
                "e" to mapOf("out" to listOf("d", "f"), "in" to listOf("c")),
                "f" to mapOf("out" to listOf(), "in" to listOf("e"))
        )

        val succ: Neighbours<String> = {graph[it]!!["out"]!!}
        val prev: Neighbours<String> = {graph[it]!!["in"]!!}
        shortestPath("a", "e", succ, prev)!!.toList() shouldBe listOf("a", "b", "c", "e")
        shortestPath("e", "b", succ, prev)!!.toList() shouldBe listOf("e", "d", "b")
        shortestPath("e", "a", succ, prev) shouldBe null
    }
}
