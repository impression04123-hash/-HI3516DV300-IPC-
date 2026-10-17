#ifndef HISI_RTSP_QUEUE_H
#define HISI_RTSP_QUEUE_H

#include "hi_comm_venc.h"

#ifdef __cplusplus
extern "C" {
#endif

HI_S32 RtspQueue_Start(void);
HI_VOID RtspQueue_Stop(void);
/* Caller retains the SDK stream and must still call ReleaseStream on failure. */
HI_S32 saveStream(VENC_STREAM_S *pstStream);
HI_VOID *vdRTPSendThread(HI_VOID *arg);

#ifdef __cplusplus
}
#endif

#endif
