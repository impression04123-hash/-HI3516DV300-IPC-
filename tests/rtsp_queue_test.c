#include <assert.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "rtsp_queue.h"

/* Only allocation, thread-creation failure and the external network sink are
 * controlled. The production queue and the board SDK's VENC types are real. */
void *__real_malloc(size_t size);
void __real_free(void *ptr);
int __real_pthread_create(pthread_t *, const pthread_attr_t *,
                          void *(*)(void *), void *);

static pthread_mutex_t allocation_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t allocation_changed = PTHREAD_COND_INITIALIZER;
static void *allocations[256];
static int live_allocations;
static int fail_allocation;
static int fail_thread;

void *__wrap_malloc(size_t size)
{
    void *ptr;
    unsigned int i;
    pthread_mutex_lock(&allocation_lock);
    if (fail_allocation && --fail_allocation == 0) {
        pthread_mutex_unlock(&allocation_lock);
        return NULL;
    }
    ptr = __real_malloc(size);
    assert(ptr != NULL);
    for (i = 0; i < 256 && allocations[i] != NULL; ++i) {}
    assert(i < 256);
    allocations[i] = ptr;
    ++live_allocations;
    pthread_mutex_unlock(&allocation_lock);
    return ptr;
}

void __wrap_free(void *ptr)
{
    unsigned int i;
    if (ptr == NULL) return;
    pthread_mutex_lock(&allocation_lock);
    for (i = 0; i < 256 && allocations[i] != ptr; ++i) {}
    assert(i < 256); /* Includes double-free and foreign ownership checks. */
    allocations[i] = NULL;
    --live_allocations;
    __real_free(ptr);
    pthread_cond_broadcast(&allocation_changed);
    pthread_mutex_unlock(&allocation_lock);
}

int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                          void *(*entry)(void *), void *arg)
{
    if (fail_thread) return 11;
    return __real_pthread_create(thread, attr, entry, arg);
}

static struct timespec deadline(void)
{
    struct timespec result;
    assert(clock_gettime(CLOCK_REALTIME, &result) == 0);
    result.tv_sec += 10;
    return result;
}

static void wait_allocations(int expected)
{
    struct timespec until = deadline();
    pthread_mutex_lock(&allocation_lock);
    while (live_allocations != expected)
        assert(pthread_cond_timedwait(&allocation_changed, &allocation_lock,
                                     &until) == 0);
    pthread_mutex_unlock(&allocation_lock);
}

static pthread_mutex_t sink_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sink_changed = PTHREAD_COND_INITIALIZER;
static int sink_blocked;
static int sink_entered;
static int sink_count;
static unsigned char sent[32768][6];

HI_S32 VENC_Sent(char *buffer, int length)
{
    pthread_mutex_lock(&sink_lock);
    sink_entered = 1;
    pthread_cond_broadcast(&sink_changed);
    while (sink_blocked)
        pthread_cond_wait(&sink_changed, &sink_lock);
    assert(length == 6);
    assert(sink_count < 32768);
    memcpy(sent[sink_count++], buffer, 6);
    pthread_cond_broadcast(&sink_changed);
    pthread_mutex_unlock(&sink_lock);
    return HI_FAILURE; /* A failed send must release the frame as well. */
}

static void reset_sink(int blocked)
{
    pthread_mutex_lock(&sink_lock);
    sink_blocked = blocked;
    sink_entered = 0;
    sink_count = 0;
    memset(sent, 0, sizeof(sent));
    pthread_mutex_unlock(&sink_lock);
}

static void wait_entered(void)
{
    struct timespec until = deadline();
    pthread_mutex_lock(&sink_lock);
    while (!sink_entered)
        assert(pthread_cond_timedwait(&sink_changed, &sink_lock, &until) == 0);
    pthread_mutex_unlock(&sink_lock);
}

static void release_sink(void)
{
    pthread_mutex_lock(&sink_lock);
    sink_blocked = 0;
    pthread_cond_broadcast(&sink_changed);
    pthread_mutex_unlock(&sink_lock);
}

static void wait_last_id(int id, int packs)
{
    struct timespec until = deadline();
    pthread_mutex_lock(&sink_lock);
    while (sink_count < packs || sent[sink_count - 1][5] != id ||
           sink_count % packs != 0)
        assert(pthread_cond_timedwait(&sink_changed, &sink_lock, &until) == 0);
    pthread_mutex_unlock(&sink_lock);
}

typedef struct {
    VENC_STREAM_S stream;
    VENC_PACK_S packs[3];
    unsigned char bytes[3][16];
} Fixture;

static void frame(Fixture *f, unsigned char id, int count)
{
    int i;
    memset(f, 0, sizeof(*f));
    f->stream.pstPack = f->packs;
    f->stream.u32PackCount = count;
    for (i = 0; i < count; ++i) {
        unsigned char payload[6] = {0, 0, 0, 1, 0x61, id};
        if (count == 3) payload[4] = i == 0 ? 0x67 : (i == 1 ? 0x68 : 0x65);
        memset(f->bytes[i], 0xaa, sizeof(f->bytes[i]));
        memcpy(f->bytes[i] + 2, payload, sizeof(payload));
        f->packs[i].pu8Addr = f->bytes[i];
        f->packs[i].u32Offset = 2;
        f->packs[i].u32Len = 8;
        f->packs[i].bFrameEnd = (i == count - 1) ? HI_TRUE : HI_FALSE;
    }
}

static void assert_frame(int index, int id)
{
    const unsigned char expected[3][6] = {
        {0, 0, 0, 1, 0x67, 0}, {0, 0, 0, 1, 0x68, 0},
        {0, 0, 0, 1, 0x65, 0}
    };
    int i;
    for (i = 0; i < 3; ++i) {
        assert(memcmp(sent[index + i], expected[i], 5) == 0);
        assert(sent[index + i][5] == id);
    }
}

static void test_offsets_deep_copy_and_pack_boundaries(void)
{
    Fixture f;
    reset_sink(1);
    assert(RtspQueue_Start() == HI_SUCCESS);
    frame(&f, 42, 3);
    assert(saveStream(&f.stream) == HI_SUCCESS);
    wait_entered();
    /* Models MP4 rewriting SDK memory followed by ReleaseStream/reuse. */
    memset(&f, 0xee, sizeof(f));
    release_sink();
    wait_last_id(42, 3);
    RtspQueue_Stop();
    assert(sink_count == 3);
    assert_frame(0, 42);
    wait_allocations(0);
    puts("PASS offsets, full-frame deep copy, original pack boundaries, send failure cleanup");
}

static void test_five_pending_frames_drop_oldest(void)
{
    Fixture f;
    const int expected[] = {10, 21, 22, 23, 24, 25};
    int i;
    reset_sink(1);
    assert(RtspQueue_Start() == HI_SUCCESS);
    frame(&f, 10, 3);
    assert(saveStream(&f.stream) == HI_SUCCESS);
    wait_entered();
    for (i = 20; i <= 25; ++i) {
        frame(&f, i, 3);
        assert(saveStream(&f.stream) == HI_SUCCESS);
    }
    release_sink();
    wait_last_id(25, 3);
    RtspQueue_Stop();
    assert(sink_count == 18);
    for (i = 0; i < 6; ++i) assert_frame(i * 3, expected[i]);
    wait_allocations(0);
    puts("PASS five pending frames, whole-frame oldest eviction, in-flight ownership");
}

static void test_invalid_frame_and_allocation_failures(void)
{
    Fixture f;
    int i;
    reset_sink(0);
    assert(RtspQueue_Start() == HI_SUCCESS);
    assert(saveStream(NULL) == HI_FAILURE);
    frame(&f, 50, 3);
    f.stream.u32PackCount = 0;
    assert(saveStream(&f.stream) == HI_FAILURE);
    frame(&f, 50, 3);
    f.stream.pstPack = NULL;
    assert(saveStream(&f.stream) == HI_FAILURE);
    for (i = 0; i < 6; ++i) {
        frame(&f, 50, 3);
        if (i == 0) f.packs[1].u32Offset = 9;
        if (i == 1) f.packs[1].u32Offset = 8;
        if (i == 2) f.packs[1].pu8Addr = NULL;
        if (i == 3) f.packs[2].bFrameEnd = HI_FALSE;
        if (i == 4) f.packs[0].bFrameEnd = HI_TRUE;
        if (i == 5) f.packs[1].u32Len = UINT_MAX;
        assert(saveStream(&f.stream) == HI_FAILURE);
        wait_allocations(0);
    }
    for (i = 1; i <= 3; ++i) {
        frame(&f, 50, 3);
        pthread_mutex_lock(&allocation_lock);
        fail_allocation = i;
        pthread_mutex_unlock(&allocation_lock);
        assert(saveStream(&f.stream) == HI_FAILURE);
        wait_allocations(0);
    }
    RtspQueue_Stop();
    assert(sink_count == 0); /* No prefix of an invalid frame was published. */
    puts("PASS invalid/partial frames, overflow, every allocation failure is atomic and leak-free");
}

static void *stopper(void *unused)
{
    (void)unused;
    RtspQueue_Stop();
    return NULL;
}

static void test_shutdown_with_pending_and_inflight_frames(void)
{
    Fixture f;
    pthread_t thread;
    int i;
    reset_sink(1);
    assert(RtspQueue_Start() == HI_SUCCESS);
    frame(&f, 60, 3);
    assert(saveStream(&f.stream) == HI_SUCCESS);
    wait_entered();
    for (i = 61; i < 64; ++i) {
        frame(&f, i, 3);
        assert(saveStream(&f.stream) == HI_SUCCESS);
    }
    assert(pthread_create(&thread, NULL, stopper, NULL) == 0);
    wait_allocations(3); /* Pending frames gone; in-flight frame still owned. */
    assert(saveStream(&f.stream) == HI_FAILURE);
    release_sink();
    assert(pthread_join(thread, NULL) == 0);
    assert(sink_count == 3);
    assert_frame(0, 60);
    wait_allocations(0);
    RtspQueue_Stop(); /* Idempotent stop, also safe before start. */
    puts("PASS stop drains pending frames, joins in-flight sender, rejects new frames");
}

static void *producer(void *arg)
{
    Fixture f;
    int i;
    for (i = 0; i < 2000; ++i) {
        frame(&f, 70 + (uintptr_t)arg, 3);
        assert(saveStream(&f.stream) == HI_SUCCESS);
    }
    return NULL;
}

static void test_concurrent_producers(void)
{
    Fixture f;
    pthread_t threads[4];
    int i;
    reset_sink(1);
    assert(RtspQueue_Start() == HI_SUCCESS);
    assert(RtspQueue_Start() == HI_SUCCESS); /* Does not create a second sender. */
    frame(&f, 69, 3);
    assert(saveStream(&f.stream) == HI_SUCCESS);
    wait_entered();
    for (i = 0; i < 4; ++i)
        assert(pthread_create(&threads[i], NULL, producer, (void *)(uintptr_t)i) == 0);
    for (i = 0; i < 4; ++i) assert(pthread_join(threads[i], NULL) == 0);
    frame(&f, 255, 3);
    assert(saveStream(&f.stream) == HI_SUCCESS);
    release_sink();
    wait_last_id(255, 3);
    RtspQueue_Stop();
    assert(sink_count == 18);
    assert_frame(0, 69);
    for (i = 3; i < sink_count; i += 3) assert_frame(i, sent[i][5]);
    assert_frame(15, 255);
    wait_allocations(0);
    puts("PASS 8000 frames from four concurrent producers, bounded queue and intact frames");
}

static void test_start_failure_and_restart(void)
{
    Fixture f;
    reset_sink(0);
    frame(&f, 90, 1);
    assert(saveStream(&f.stream) == HI_FAILURE);
    fail_thread = 1;
    assert(RtspQueue_Start() == HI_FAILURE);
    fail_thread = 0;
    assert(saveStream(&f.stream) == HI_FAILURE);
    RtspQueue_Stop();
    assert(RtspQueue_Start() == HI_SUCCESS);
    assert(saveStream(&f.stream) == HI_SUCCESS);
    wait_last_id(90, 1);
    RtspQueue_Stop();
    assert(sink_count == 1);
    wait_allocations(0);
    puts("PASS thread creation failure, stopped state, single-pack frame and restart");
}

static void test_concurrent_producers_and_sender(void)
{
    Fixture f;
    pthread_t threads[4];
    int i;
    reset_sink(0);
    assert(RtspQueue_Start() == HI_SUCCESS);
    for (i = 0; i < 4; ++i)
        assert(pthread_create(&threads[i], NULL, producer, (void *)(uintptr_t)i) == 0);
    for (i = 0; i < 4; ++i) assert(pthread_join(threads[i], NULL) == 0);
    frame(&f, 255, 3);
    assert(saveStream(&f.stream) == HI_SUCCESS);
    wait_last_id(255, 3);
    RtspQueue_Stop();
    assert(sink_count >= 3 && sink_count <= 24003 && sink_count % 3 == 0);
    for (i = 0; i < sink_count; i += 3) assert_frame(i, sent[i][5]);
    assert_frame(sink_count - 3, 255);
    wait_allocations(0);
    puts("PASS 8000 frames while producer enqueue, overflow eviction and sender dequeue overlap");
}

int main(void)
{
    RtspQueue_Stop();
    test_offsets_deep_copy_and_pack_boundaries();
    test_five_pending_frames_drop_oldest();
    test_invalid_frame_and_allocation_failures();
    test_shutdown_with_pending_and_inflight_frames();
    test_concurrent_producers();
    test_concurrent_producers_and_sender();
    test_start_failure_and_restart();
    puts("All RTSP queue tests passed.");
    return 0;
}
