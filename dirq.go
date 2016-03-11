/*
 * Copyright (c) CERN 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dirq

//#cgo pkg-config: --static dirq
//#include <dirq.h>
//#include <stdlib.h>
//#include <string.h>
//static __thread const char* buffer;
//static __thread size_t offset, length;
//
//static int dirq_iow_callback(dirq_t dirq, char *out, size_t outsize)
//{
//	if (offset > length)
//		return 0;
//	int remaining = length - offset;
//	if (outsize > remaining)
//		outsize = remaining;
//	strncpy(out, buffer + offset, outsize);
//	offset += outsize;
//	return outsize;
//}
//
//static const char* dirq_add_wrapper(dirq_t dirq, const char *msg, size_t len)
//{
//	buffer = msg;
//	offset = 0;
//	length = len;
//	return dirq_add(dirq, dirq_iow_callback);
//}
import "C"
import (
	"fmt"
	"io/ioutil"
	"unsafe"
)

type Dirq struct {
	handle C.dirq_t
}

type DirqMsg struct {
	Message []byte
	Error   error
}

// Construct a new DirQ handle.
func New(path string) (dirq Dirq, err error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	dirq.handle = C.dirq_new(cPath)
	if C.dirq_get_errcode(dirq.handle) != 0 {
		errStr := C.GoString(C.dirq_get_errstr(dirq.handle))
		err = fmt.Errorf("Failed to create the DirQ handle: %s", errStr)
	}
	return
}

// Frees the memory associated with the dirq handle.
func (dirq *Dirq) Close() {
	C.dirq_free(dirq.handle)
}

// Consume messages on the DirQ directory. For long running processes,
// you may need to call this periodically, since the channel will be closed once it is out of
// messages, and you will lose any other coming in later.
func (dirq *Dirq) Consume() <-chan DirqMsg {
	messages := make(chan DirqMsg)
	go func() {
		iter := C.dirq_first(dirq.handle)
		for iter != nil {
			var msg DirqMsg

			if C.dirq_lock(dirq.handle, iter, 0) == 0 {
				path := C.dirq_get_path(dirq.handle, iter)
				msg.Message, msg.Error = ioutil.ReadFile(C.GoString(path))
				C.dirq_remove(dirq.handle, iter)
				messages <- msg
			}

			iter = C.dirq_next(dirq.handle)
		}
		close(messages)
	}()
	return messages
}

// Produce a single message.
func (dirq *Dirq) Produce(msg []byte) error {
	C.dirq_add_wrapper(dirq.handle, (*C.char)(unsafe.Pointer(&msg[0])), C.size_t(len(msg)))
	if C.dirq_get_errcode(dirq.handle) != 0 {
		errStr := C.GoString(C.dirq_get_errstr(dirq.handle))
		return fmt.Errorf("Failed to produce a message: %s", errStr)
	}
	return nil
}

// Clean old directories.
func (dirq *Dirq) Purge() error {
	if C.dirq_purge(dirq.handle) < 0 {
		errStr := C.GoString(C.dirq_get_errstr(dirq.handle))
		return fmt.Errorf("Failed to purge the directory queue: %s", errStr)
	}
	return nil
}
