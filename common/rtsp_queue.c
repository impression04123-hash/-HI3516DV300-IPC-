#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "sample_comm.h"

#if RTSP_ENABLE

#define RTP_QUEUE_MAX_FRAMES 5

/*
 * One list node owns one VENC pack's copied bytes.
 * is_last_pack marks the final pack of the same complete encoded frame.
 * All packs from one saveStream() are published atomically.
 */
typedef struct _rtpbuf {
    struct list_head list;
    char *buf;
    HI_S32 len;
    HI_BOOL is_last_pack;
} RTPbuf_s;

static struct list_head RTPbuf_head = LIST_HEAD_INIT(RTPbuf_head);
static HI_U32 count;        /* Number of pending packs. */
static HI_U32 frame_count;  /* Number of complete pending frames. */

static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t queue_ready = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t lifecycle_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t gs_RtpPid;
static HI_BOOL queue_running = HI_FALSE;
static HI_BOOL sender_started = HI_FALSE;

extern HI_S32 VENC_Sent(char *buffer, int buflen);

static void free_rtp_list(struct list_head *head)
{
    while (!list_empty(head)) {
        RTPbuf_s *p = get_first_item(head, RTPbuf_s, list);
        list_del(&p->list);
        free(p->buf);
        free(p);
    }
}

/* Caller must hold queue_mutex and ensure the queue has a complete frame.
 * The detached packs belong exclusively to 'out' after this function.
 */
static void detach_oldest_frame(struct list_head *out)
{
    HI_BOOL end;

    do {
        RTPbuf_s *p = get_first_item(&RTPbuf_head, RTPbuf_s, list);
        end = p->is_last_pack;
        list_del(&p->list);
        list_add_tail(&p->list, out);
        --count;
    } while (!end);

    --frame_count;
}

HI_S32 saveStream(VENC_STREAM_S *pstStream)
{
    HI_U32 i;
    HI_U32 new_count = 0;
    struct list_head incoming;
    struct list_head discarded;
    RTPbuf_s *last = NULL;

    INIT_LIST_HEAD(&incoming);
    INIT_LIST_HEAD(&discarded);

    if (pstStream == NULL || pstStream->pstPack == NULL ||
        pstStream->u32PackCount == 0)
        return HI_FAILURE;

    /* By-frame H.264 producer: reject incomplete or malformed frames. */
    for (i = 0; i < pstStream->u32PackCount; ++i) {
        const VENC_PACK_S *src = &pstStream->pstPack[i];
        HI_U32 len;

        if (src->pu8Addr == NULL ||
            src->u32Offset >= src->u32Len ||
            src->bFrameEnd != (i == pstStream->u32PackCount - 1 ?
                              HI_TRUE : HI_FALSE))
            return HI_FAILURE;

        len = src->u32Len - src->u32Offset;
        if (len > INT_MAX)
            return HI_FAILURE;
    }

    /*
     * Build the entire frame off-queue. If an allocation fails,
     * no portion of the frame is visible to the sender.
     * Do this before MP4 can mutate the original VENC data.
     */
    for (i = 0; i < pstStream->u32PackCount; ++i) {
        const VENC_PACK_S *src = &pstStream->pstPack[i];
        HI_U32 len = src->u32Len - src->u32Offset;
        RTPbuf_s *p = malloc(sizeof(*p));

        if (p == NULL)
            goto error;

        INIT_LIST_HEAD(&p->list);
        p->buf = malloc(len);
        if (p->buf == NULL) {
            free(p);
            goto error;
        }

        p->len = (HI_S32)len;
        p->is_last_pack = HI_FALSE;
        memcpy(p->buf, src->pu8Addr + src->u32Offset, len);

        list_add_tail(&p->list, &incoming);
        last = p;
        ++new_count;
    }

    last->is_last_pack = HI_TRUE;

    pthread_mutex_lock(&queue_mutex);

    if (!queue_running || new_count > UINT_MAX - count) {
        pthread_mutex_unlock(&queue_mutex);
        goto error;
    }

    /* At most five complete frames can wait in the shared queue. */
    if (frame_count >= RTP_QUEUE_MAX_FRAMES)
        detach_oldest_frame(&discarded);

    /* Publish every pack of this frame under the same lock. */
    while (!list_empty(&incoming)) {
        RTPbuf_s *p = get_first_item(&incoming, RTPbuf_s, list);
        list_del(&p->list);
        list_add_tail(&p->list, &RTPbuf_head);
    }
    count += new_count;
    ++frame_count;

    pthread_cond_signal(&queue_ready);
    pthread_mutex_unlock(&queue_mutex);

    /* Removed packs cannot be observed by another queue consumer. */
    free_rtp_list(&discarded);
    return HI_SUCCESS;

error:
    free_rtp_list(&incoming);
    return HI_FAILURE;
}

HI_VOID *vdRTPSendThread(HI_VOID *arg)
{
    (void)arg;

    for (;;) {
        struct list_head sending;
        RTPbuf_s *p;

        INIT_LIST_HEAD(&sending);
        pthread_mutex_lock(&queue_mutex);

        while (queue_running && list_empty(&RTPbuf_head))
            pthread_cond_wait(&queue_ready, &queue_mutex);

        if (!queue_running) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }

        /* Detach the whole frame; no networking while holding the lock. */
        detach_oldest_frame(&sending);
        pthread_mutex_unlock(&queue_mutex);

        while (!list_empty(&sending)) {
            p = get_first_item(&sending, RTPbuf_s, list);
            list_del(&p->list);
            VENC_Sent(p->buf, p->len);
            free(p->buf);
            free(p);
        }
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
    struct list_head discarded;

    INIT_LIST_HEAD(&discarded);
    pthread_mutex_lock(&lifecycle_mutex);
    pthread_mutex_lock(&queue_mutex);

    queue_running = HI_FALSE;
    pthread_cond_broadcast(&queue_ready);

    while (!list_empty(&RTPbuf_head)) {
        RTPbuf_s *p = get_first_item(&RTPbuf_head, RTPbuf_s, list);
        list_del(&p->list);
        list_add_tail(&p->list, &discarded);
    }
    count = 0;
    frame_count = 0;

    pthread_mutex_unlock(&queue_mutex);
    free_rtp_list(&discarded);

    if (sender_started) {
        pthread_join(gs_RtpPid, NULL);
        sender_started = HI_FALSE;
    }
    pthread_mutex_unlock(&lifecycle_mutex);
}

#endif
