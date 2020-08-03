package com.iprcom

import cyclops.data.Seq
import java.util.*

typealias Neighbours<N> = (N) -> Iterable<N>

/*
A bi-directional breadth-first search for shortest path. Efficiently uses the 'visited' maps to hold the partial paths.
TODO upgrade to take a max path length
TODO upgrade to yield all paths of the same length
*/
fun <N> shortestPath(
        start: N,
        end: N,
        succ: Neighbours<N>, // out links
        prev: Neighbours<N>, // in links
        maxDepth: Int = Int.MAX_VALUE
): Seq<N>? {
    val seenitstart = mutableMapOf(start to Seq.of(start))
    val seenitend = mutableMapOf(end to Seq.of(end));
    val qs = LinkedList(listOf(start))
    val qe = LinkedList(listOf(end))

    fun takestep(
        qworking: LinkedList<N>,
        seenworking: MutableMap<N, Seq<N>>,
        seenother: MutableMap<N, Seq<N>>,
        neighbours: Neighbours<N>
    ) : Seq<N>? {
        val item = qworking.pop()
        if (seenother.contains(item)) {
            return seenitstart[item]!!.drop(1).reverse().appendAll(seenitend[item]!!)
        }
        val itempath = seenworking[item]!!
        for (next in neighbours(item)) {
            if (! seenworking.containsKey(next)) {
                qworking.add(next)
                seenworking[next] = Seq.cons(next, itempath)
            }
        }

        return null
    }

    while (qs.isNotEmpty() && qe.isNotEmpty()) {
        val path = if (qs.size < qe.size) { // work on the side with the smallest branching first
            takestep(qs, seenitstart, seenitend, succ)
        } else {
            takestep(qe, seenitend, seenitstart, prev)
        }

        if (path != null) return path // found a path, we're done
    }

    return null // never found a path...
}