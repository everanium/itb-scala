// The maintenance operations that mutate a live Pipeline handle
// between iterations: master rotation (--rekey-every) and blob reopen
// (--blob-cycle-every).

package io.github.everanium.itb3.scala.loop

import io.github.everanium.itb3.scala.Pipeline

object Ops:

  /** Byte length of each fresh master drawn for a rotation. Matches the
    * size Init auto-generates for both the parallax and the wrapper
    * master.
    */
  private val RekeyMasterSize = 32

  /** The empty master a disabled layer passes; Rekey ignores it. */
  private val NoMaster: Array[Byte] = Array.empty

  /** Master rotation. Rotates the parallax + wrapper masters on every
    * active Pipeline under the write lock and retains the refreshed
    * blob for subsequent blob reopens. Masters are drawn fresh from the
    * OS CSPRNG on every rotation regardless of --seed (master rotation
    * is pipeline keying, not plaintext content); a disabled layer
    * passes no bytes, which Rekey ignores. The eight inner seeds and
    * the MAC key are untouched by design — Rekey targets only the two
    * outer-layer master secrets.
    */
  private def rekeyPipes(r: RunState, id: Int, iter: Long): Boolean =
    var perm = NoMaster
    var wrap = NoMaster
    if r.cfg.parallax then
      perm = new Array[Byte](RekeyMasterSize)
      if !Payload.fillRandom(perm) then
        Worker.fail(r, id, s"g$id iter $iter: csprng: parallax master")
        return false
    if r.cfg.wrapper then
      wrap = new Array[Byte](RekeyMasterSize)
      if !Payload.fillRandom(wrap) then
        Worker.fail(r, id, s"g$id iter $iter: csprng: wrapper master")
        return false

    r.pipesLock.writeLock().lock()
    try
      r.pipes.stream match
        case Some(pipe) =>
          pipe.rekey(perm, wrap) match
            case Left(e) =>
              Worker.fail(
                r,
                id,
                s"g$id iter $iter: Rekey(${r.streamProfile}): ${Worker.detail(e)}"
              )
              return false
            case Right(blob) => r.pipes.streamBlob = blob
        case None => ()
      r.pipes.msg match
        case Some(pipe) =>
          pipe.rekey(perm, wrap) match
            case Left(e) =>
              Worker.fail(r, id, s"g$id iter $iter: Rekey(${r.msgProfile}): ${Worker.detail(e)}")
              return false
            case Right(blob) => r.pipes.msgBlob = blob
        case None => ()
    finally r.pipesLock.writeLock().unlock()
    val n = r.rekeys.incrementAndGet()
    Main.logLine(s"rekey: g$id iter $iter rotated parallax + wrapper masters (rekey #$n)")
    true

  /** Blob reopen. Reopens every active Pipeline from its retained blob
    * under the write lock: a fresh handle is loaded from the blob, the
    * running handle is freed, and the fresh one is swapped in, so every
    * later iteration round-trips through seeds and masters that
    * survived a blob crossing. The input is the blob Init or the latest
    * Rekey handed out, not a fresh Save: that is what a receiver holds,
    * and reopening from it proves the handed-out bytes rather than the
    * live state. The blob carries the Pipeline's full shape, so no
    * override reaches the reopen. On a Load failure the running handle
    * stays and the failure aborts the run.
    */
  private def blobCyclePipes(r: RunState, id: Int, iter: Long): Boolean =
    r.pipesLock.writeLock().lock()
    try
      r.pipes.stream match
        case Some(running) =>
          Pipeline.load(r.pipes.streamBlob) match
            case Left(e) =>
              Worker.fail(r, id, s"g$id iter $iter: Load(${r.streamProfile}): ${Worker.detail(e)}")
              return false
            case Right(fresh) =>
              running.close()
              r.pipes.stream = Some(fresh)
        case None => ()
      r.pipes.msg match
        case Some(running) =>
          Pipeline.load(r.pipes.msgBlob) match
            case Left(e) =>
              Worker.fail(r, id, s"g$id iter $iter: Load(${r.msgProfile}): ${Worker.detail(e)}")
              return false
            case Right(fresh) =>
              running.close()
              r.pipes.msg = Some(fresh)
        case None => ()
    finally r.pipesLock.writeLock().unlock()
    val n = r.blobCycles.incrementAndGet()
    Main.logLine(s"blob-cycle: g$id iter $iter reopened from session blob (cycle #$n)")
    true

  /** Handle mutation. Runs the periodic Pipeline-mutating operations
    * after a completed iteration: master rotation (--rekey-every) and
    * blob reopen (--blob-cycle-every). Both intervals count per-worker
    * iterations; the warmup iteration (iter 0) never triggers because
    * the worker loop calls this for iter >= 1 only. Rekey rewrites the
    * outer-layer keying of a live handle and a blob reopen replaces the
    * handle outright; each takes the write lock, so in-flight cipher
    * calls on other workers drain before anything changes and no
    * encrypt is separated from its decrypt by either. False after
    * recording the worker error.
    */
  def maintenance(r: RunState, id: Int, iter: Long): Boolean =
    val cfg = r.cfg
    if cfg.rekeyEvery > 0 && iter % cfg.rekeyEvery == 0 && !rekeyPipes(r, id, iter) then false
    else if cfg.blobCycleEvery > 0 && iter % cfg.blobCycleEvery == 0 &&
      !blobCyclePipes(r, id, iter)
    then false
    else true
