package com.iprcom

import clojure.lang.RT
import datomic.Util
import io.kotest.core.spec.style.AnnotationSpec
import io.kotest.matchers.shouldBe

class UtilTest : AnnotationSpec() {

    @Test
    fun keywords_test() {
        keyword(":foo") shouldBe RT.keyword(null, "foo")
        keyword(":foo") shouldBe Util.read(":foo")
    }
}