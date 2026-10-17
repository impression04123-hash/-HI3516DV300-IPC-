#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "sample_comm.h"

#if RTSP_ENABLE

#define RTP_QUEUE_MAX_FRAMES 5

/* One node owns one complete SDK frame. A pack may contain several NAL units;
 * preserve the original pack boundaries expected by the existing VENC_Sent. */
typedef struct _rtpbuf {
    struct list_head list;
    HI_S32 len;
    char *buf;
    HI_U32 pack_count;
    HI_U32 *pack_lens;
} RTPbuf_s;

static struct list_head RTPbuf_head = LIST_HEAD_INIT(RTPbuf_head);
static HI_U32 count;
static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t queue_ready = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t lifecycle_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t gs_RtpPid;
static HI_BOOL queue_running = HI_FALSE;
static HI_BOOL sender_started = HI_FALSE;

/* This function fans out to clients; producers must never enqueue per client. */
extern HI_S32 VENC_Sent(char *buffer, int buflen);

static void freeRtpFrame(RTPbuf_s *frame)
{
    if (frame != NULL) {
        free(frame->pack_lens);
        free(frame->buf);
        free(frame);
    }
}

/* queue_mutex must be held. Detached frames belong exclusively to the caller. */
static RTPbuf_s *popRtpFrame(void)
{
    RTPbuf_s *frame = get_first_item(&RTPbuf_head, RTPbuf_s, list);
    list_del(&frame->list);
    --count;
    return frame;
}

HI_S32 saveStream(VENC_STREAM_S *pstStream)
{
    HI_U32 i;
    size_t total = 0;
    size_t offset = 0;
    RTPbuf_s *frame;
    RTPbuf_s *oldest = NULL;

    if (pstStream == NULL || pstStream->pstPack == NULL ||
        pstStream->u32PackCount == 0 ||
        SIZE_MAX / pstStream->u32PackCount < sizeof(HI_U32))
        return HI_FAILURE;

    /* The caller only supplies H.264 streams in bByFrame mode. Validate the
     * complete frame before allocation/publication; never queue a partial one. */
    for (i = 0; i < pstStream->u32PackCount; ++i) {
        const VENC_PACK_S *pack = &pstStream->pstPack[i];
        size_t length;
        if (pack->pu8Addr == NULL || pack->u32Offset >= pack->u32Len ||
            pack->bFrameEnd != (i == pstStream->u32PackCount - 1 ? HI_TRUE : HI_FALSE))
            return HI_FAILURE;
        length = pack->u32Len - pack->u32Offset;
        if (length > (size_t)INT_MAX - total)
            return HI_FAILURE;
        total += length;
    }

    frame = malloc(sizeof(*frame));
    if (frame == NULL) return HI_FAILURE;
    INIT_LIST_HEAD(&frame->list);
    frame->buf = NULL;
    frame->pack_lens = NULL;
    frame->len = (HI_S32)total;
    frame->pack_count = pstStream->u32PackCount;
    frame->pack_lens = malloc((size_t)frame->pack_count * sizeof(*frame->pack_lens));
    if (frame->pack_lens == NULL) {
        freeRtpFrame(frame);
        return HI_FAILURE;
    }
    frame->buf = malloc(total);
    if (frame->buf == NULL) {
        freeRtpFrame(frame);
        return HI_FAILURE;
    }

    /* Own the bytes before the caller records MP4 (which rewrites start codes)
     * or releases the vendor stream. No SDK pointers survive this function. */
    for (i = 0; i < frame->pack_count; ++i) {
        const VENC_PACK_S *pack = &pstStream->pstPack[i];
        frame->pack_lens[i] = pack->u32Len - pack->u32Offset;
        memcpy(frame->buf + offset, pack->pu8Addr + pack->u32Offset,
               frame->pack_lens[i]);
        offset += frame->pack_lens[i];
    }

    pthread_mutex_lock(&queue_mutex);
    if (!queue_running) {
        pthread_mutex_unlock(&queue_mutex);
        freeRtpFrame(frame);
        return HI_FAILURE;
    }
    if (count == RTP_QUEUE_MAX_FRAMES)
        oldest = popRtpFrame();
    list_add_tail(&frame->list, &RTPbuf_head);
    ++count;
    pthread_cond_signal(&queue_ready);
    pthread_mutex_unlock(&queue_mutex);
    freeRtpFrame(oldest);
    return HI_SUCCESS;
}

HI_VOID *vdRTPSendThread(HI_VOID *arg)
{
    (void)arg;
    for (;;) {
        RTPbuf_s *frame;
        HI_U32 i;
        size_t offset = 0;
        pthread_mutex_lock(&queue_mutex);
        while (queue_running && list_empty(&RTPbuf_head))
            pthread_cond_wait(&queue_ready, &queue_mutex);
        if (!queue_running) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }
        frame = popRtpFrame();
        pthread_mutex_unlock(&queue_mutex);

        /* Network I/O never holds the queue lock. Overflow/stop cannot free
         * this frame until all its original packs have been sent. */
        for (i = 0; i < frame->pack_count; ++i) {
            VENC_Sent(frame->buf + offset, (int)frame->pack_lens[i]);
            offset += frame->pack_lens[i];
        }
        freeRtpFrame(frame);
    }
    return NULL;
}

HI_S32 RtspQueue_Start(void)
{
    int ret = 0;
    pthread_mutex_lock(&lifecycle_mutex);
    pthread_mutex_lock(&queue_mutex);
    if (!sender_started) {
        queue_running = HI_TRUE;
        ret = pthread_create(&gs_RtpPid, NULL, vdRTPSendThread, NULL);
        if (ret == 0)
            sender_started = HI_TRUE;
        else
            queue_running = HI_FALSE;
    }
    pthread_mutex_unlock(&queue_mutex);
    pthread_mutex_unlock(&lifecycle_mutex);
    return ret == 0 ? HI_SUCCESS : HI_FAILURE;
}

HI_VOID RtspQueue_Stop(void)
{
    pthread_mutex_lock(&lifecycle_mutex);
    pthread_mutex_lock(&queue_mutex);
    queue_running = HI_FALSE;
    pthread_cond_broadcast(&queue_ready);
    while (!list_empty(&RTPbuf_head))
        freeRtpFrame(popRtpFrame());
    pthread_mutex_unlock(&queue_mutex);
    if (sender_started) {
        pthread_join(gs_RtpPid, NULL);
        sender_started = HI_FALSE;
    }
    pthread_mutex_unlock(&lifecycle_mutex);
}

#endif
