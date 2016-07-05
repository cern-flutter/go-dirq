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

import (
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"
)

func BenchmarkEnqueueConcurrent(b *testing.B) {
	if err := os.Mkdir(dirqPath, 0755); err != nil && err.(*os.PathError).Err != syscall.EEXIST {
		b.Fatal(err)
	}

	dirq, err := New(dirqPath)
	if err != nil {
		b.Fatal(err)
	}
	defer dirq.Close()

	// Dequeue
	done := make(chan error)
	go func() {
		for j := 0; j < b.N; {
			if data, err := dirq.ConsumeOne(); err != nil {
				done <- err
				return
			} else if data != nil {
				j++
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
		done <- nil
	}()

	// Queue
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		transfer := []byte(fmt.Sprint(i))
		if err := dirq.Produce(transfer); err != nil {
			b.Fatal(err)
		}
	}

	// Clean
	b.StopTimer()
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	os.RemoveAll(dirqPath)
}
