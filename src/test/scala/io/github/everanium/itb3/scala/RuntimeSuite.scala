// Runtime diagnostics surface: GOMAXPROCS query / set / restore, the
// heap-profile writer, the pool-counter snapshot and its slot layout,
// and the hash-registry enumeration.

package io.github.everanium.itb3.scala

import java.nio.file.Files

class RuntimeSuite extends ItbSuite:

  test("gomaxprocs query / set / restore"):
    val orig = Runtime.setGOMAXPROCS(0)
    assert(orig > 0)
    assertEquals(Runtime.setGOMAXPROCS(-3), orig)
    assertEquals(Runtime.setGOMAXPROCS(orig + 1), orig)
    assertEquals(Runtime.setGOMAXPROCS(0), orig + 1)
    assertEquals(Runtime.setGOMAXPROCS(orig), orig + 1)

  test("heap profile written, empty path rejected"):
    val dir = Files.createTempDirectory("itb-loop-test-heap-")
    val profile = dir.resolve("heap.prof")
    ok(Runtime.writeHeapProfile(profile.toString))
    assert(Files.size(profile) > 0)
    Files.delete(profile)
    Files.delete(dir)

    // The empty path falls back to ITB_MEMPROFILE inside libitb3; the
    // test environment sets no such variable, so there is nothing to
    // fall back to.
    assertEquals(err(Runtime.writeHeapProfile("")).status, Status.BadInput)

  test("pool counter slot layout"):
    val len = Runtime.poolStatsLen()
    assert(len >= 9)
    val v = ok(Runtime.poolStats())
    assertEquals(v.length, len)
    val tiers = v(0)
    assert(tiers > 0)
    assertEquals(1 + 5 * tiers + 8, len.toLong)

  test("hash registry enumeration is canonical"):
    val names = ok(Pipeline.hashNames())
    assertEquals(names.head, "aesitb128")
    assert(names.contains("areion512"))
