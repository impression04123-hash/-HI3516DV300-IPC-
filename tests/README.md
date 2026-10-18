# RTSP queue regression tests

Run on Linux (including WSL) with GCC, GNU Make, pthreads, and the development
board SDK headers. The tests use the SDK's actual `VENC_STREAM_S`, `VENC_PACK_S`
and `list.h`, rather than guessing their layout. No board or network is required.

```sh
make -C tests SDK_INCLUDE='/path/to/mpp_smp_linux/include' test
make -C tests SDK_INCLUDE='/path/to/mpp_smp_linux/include' \
  BUILD_DIR=/tmp/hisi-rtsp-queue-asan \
  CFLAGS='-O1 -g -std=gnu99 -Wall -Wextra -Werror -pthread -fsanitize=address,undefined -fno-omit-frame-pointer' \
  LDFLAGS='-fsanitize=address,undefined' test
```

The controlled sink replaces only `VENC_Sent`, the external network operation.
Linker wrappers inject allocation/thread creation failures and track allocations.
The tests exercise the production queue: pack offsets and independent buffers,
deep copy before MP4 mutation/SDK release, invalid/partial frames, overflow,
five pending frames, oldest whole-frame eviction, in-flight ownership, all six
allocations for a three-pack frame, stop/restart, failed sends, and concurrent producers/sender.

## Queue contract and integration

- One `saveStream` call publishes at most one complete frame, independently of
  client count. The existing `VENC_Sent` handles client fan-out, including no clients.
- Five **pending** frames are allowed; the sender may additionally own one
  in-flight frame. Queue locking excludes allocation/copy and network I/O.
- Only H.264 channels configured with `bByFrame` enter this queue. Each pack's
  valid bytes are `[pu8Addr + u32Offset, pu8Addr + u32Len)`. All packs before the
  last must have `bFrameEnd == HI_FALSE`, and the last must have `HI_TRUE`.
- Each `RTPbuf_s` list node deep-copies **one** SDK pack using `malloc` and
  `memcpy`; `is_last_pack` marks the last pack in that frame. All packs for one
  frame are published together under the queue mutex. Eviction and dequeue
  detach all packs through the marked frame end, keeping a single shared list.
- A frame can contain several packs, and a pack can contain several NALs.
  The queue retains all original pack boundaries and bytes, including Annex B
  start codes. The sender invokes the existing `VENC_Sent` once per original pack.
  Concatenating a frame into one call would change that function's input contract.
- The producer copies before the MP4 recorder rewrites stream bytes, and still
  calls `HI_MPI_VENC_ReleaseStream` and frees `pstPack` after a queue drop.
- Stop the encoder producer before `RtspQueue_Stop`. Stop rejects new frames,
  discards pending frames and joins the sender after it releases its current frame.
  Start/stop calls are serialized and repeatable. An in-progress blocking network
  send must return before stop can finish.

Queue start/stop use pthread synchronization and must run in normal thread
context. They must not be called from an asynchronous signal handler. The
sample's pre-existing signal handler is otherwise unchanged by this patch.

## Verification limits

These tests validate queue behavior and memory ownership, not vendor MPI runtime
behavior, hardware decoding or end-to-end playback. The existing `VENC_Sent`
wire format/timestamps and RTSP session synchronization have not been redesigned.
Dropping reference frames may affect decoding until a later keyframe; no IDR
request/recovery policy is added by this queue fix.

Independent review also identified existing issues outside this queue change:
the MP4 recorder's `dirPath[20]` buffer is too small for its filename concatenation,
the MP4 call is not gated on H.264 payload type, and RTSP client session fields
are shared without synchronization. This patch does not claim to repair those.

The checked-in sample Makefile requires its original SDK parent files
(`Makefile.param`, architecture make rules), cross compiler and vendor/MP4
libraries. Rebuild the firmware program in that environment before deployment.
The previously committed `sample_venc` executable and `.o` files are not rebuilt
by these host tests and do not contain this source change.

Validation on 2026-10-18 with the development board SDK V2.0.2.0 headers:
all seven queue test groups passed using WSL GCC normally and under
AddressSanitizer/UBSan. Each three-pack frame now uses six separately tracked
allocations (three list nodes and three payload buffers); forced failures at
all six allocation sites were tested. This run has not repeated ThreadSanitizer,
full firmware cross-compilation or hardware playback tests. The existing
`VENC_Sent` RTP packetization, timestamps and client session synchronization
still need separate review.
