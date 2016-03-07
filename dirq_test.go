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
	"container/list"
	"os"
	"testing"
)

var dirqPath = "/tmp/dirq_test"

func produceCheck(t *testing.T, err error) {
	if err != nil {
		t.Error(err.Error())
	}
}

func TestSimpleProduceConsume(t *testing.T) {
	dirq, err := New(dirqPath)
	if err != nil {
		t.Error("Failed to open the queue directory.", err.Error())
		return
	}
	defer dirq.Close()

	var original list.List
	original.PushBack("HELLO")
	original.PushBack("GOODBYE")
	original.PushBack("HOWDY")

	for iter := original.Front(); iter != nil; iter = iter.Next() {
		err = dirq.Produce(string(iter.Value.(string)))
		if err != nil {
			t.Error("Error when producing a message.", err.Error())
		}
	}

	var messages list.List
	channel := dirq.Consume()
	for msg := range channel {
		messages.PushBack(msg.Message)
	}

	if messages.Len() != original.Len() {
		t.Errorf("Messages recovered do not match produced: %d != %d", messages.Len(), original.Len())
	}

	messages = *list.New()
	channel = dirq.Consume()
	for msg := range channel {
		messages.PushBack(msg)
	}
	if messages.Len() > 0 {
		t.Error("There must be no more entries.")
	}

	err = dirq.Purge()
	if err != nil {
		t.Error("Failed to purge.", err.Error())
	}
}

func TestMain(m *testing.M) {
	os.RemoveAll(dirqPath)
	os.Exit(m.Run())
}
