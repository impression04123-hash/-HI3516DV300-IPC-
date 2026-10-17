CC ?= cc
BUILD_DIR ?= /tmp/hisi-rtsp-queue-tests
CFLAGS ?= -O1 -g
CPPFLAGS += -I../common -isystem "$(SDK_INCLUDE)"
CFLAGS += -std=gnu99 -Wall -Wextra -Werror -pthread
WRAPS = -Wl,--wrap=malloc,--wrap=free,--wrap=pthread_create

.PHONY: test clean
test: $(BUILD_DIR)/rtsp_queue_test
	$(BUILD_DIR)/rtsp_queue_test

$(BUILD_DIR)/rtsp_queue_test: rtsp_queue_test.c ../common/rtsp_queue.c ../common/rtsp_queue.h ../common/sample_comm.h
	@test -f "$(SDK_INCLUDE)/hi_comm_venc.h" || (echo 'Set SDK_INCLUDE to the board SDK include directory'; exit 1)
	mkdir -p "$(BUILD_DIR)"
	$(CC) $(CPPFLAGS) $(CFLAGS) rtsp_queue_test.c ../common/rtsp_queue.c $(WRAPS) $(LDFLAGS) -o "$@"

clean:
	rm -f "$(BUILD_DIR)/rtsp_queue_test"
