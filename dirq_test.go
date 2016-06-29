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
	"path"
	"reflect"
	"testing"
	"time"
)

var dirqPath = "/tmp/dirq_test"

// Produce and consume 3 messages
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
		err = dirq.Produce([]byte(iter.Value.(string)))
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
}

// Produce and consume a message that has an embedded zero
func TestAZero(t *testing.T) {
	dirq, err := New(dirqPath)
	if err != nil {
		t.Error("Failed to open the queue directory.", err.Error())
		return
	}
	defer dirq.Close()

	original := []byte{'a', 'b', 'c', 0x00, 'd', 'e'}
	dirq.Produce(original)

	consumed := <-dirq.Consume()
	if consumed.Error != nil {
		t.Error("Failed to consume: ", consumed.Error.Error())
		return
	}

	if !reflect.DeepEqual(consumed.Message, original) {
		t.Error("Consumed message does not match the generated one")
	}
}

// Test purging
func TestPurge(t *testing.T) {
	dirq, err := New(dirqPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = dirq.Purge(); err != nil {
		t.Error(err)
	}
}

// Test purging with files to be deleted
func TestPurge2(t *testing.T) {
	dirOk := path.Join(dirqPath, "12345678")
	dirToBeRemoved := path.Join(dirqPath, "12345abc")

	if err := os.MkdirAll(dirOk, os.FileMode(0775)); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dirToBeRemoved, os.FileMode(0775)); err != nil {
		t.Fatal(err)
	}

	// Old lock
	lock := path.Join(dirOk, "54321.lck")
	if f, err := os.Create(lock); err != nil {
		t.Fatal(err)
	} else {
		f.Close()
	}

	// Old temp
	temp := path.Join(dirOk, "abcdef.tmp")
	if f, err := os.Create(temp); err != nil {
		t.Fatal(err)
	} else {
		f.Close()
	}

	// Fine file
	okFile := path.Join(dirOk, "1234ab")
	if f, err := os.Create(okFile); err != nil {
		t.Fatal(err)
	} else {
		f.Close()
	}

	// Give time
	time.Sleep(2 * time.Second)

	// Newer temp
	newTemp := path.Join(dirOk, "fedcba.tmp")
	if f, err := os.Create(newTemp); err != nil {
		t.Fatal(err)
	} else {
		f.Close()
	}

	// Create dirq with short lifetimes
	dirq := &Dirq{
		Path:        dirqPath,
		Umask:       0022,
		MaxLockLife: 1 * time.Second,
		MaxTempLife: 1 * time.Second,
	}

	// Purge
	if err := dirq.Purge(); err != nil {
		t.Fatal(err)
	}

	// Check directories
	if _, err := os.Stat(dirToBeRemoved); !os.IsNotExist(err) {
		t.Error("Empty directory should have been removed, ", err)
	}
	if _, err := os.Stat(dirOk); err != nil {
		t.Error(err)
	}

	// Check files
	if _, err := os.Stat(lock); !os.IsNotExist(err) {
		t.Error("Lock file should have been removed, ", err)
	}
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Error("Temp file should have been removed, ", err)
	}
	if _, err := os.Stat(okFile); err != nil {
		t.Error("File must remain there, ", err)
	}
	if _, err := os.Stat(newTemp); err != nil {
		t.Error("Newer temp file must remain there, ", err)
	}
}

// Test the ConsumeOne call
func TestConsumeOne(t *testing.T) {
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
		err = dirq.Produce([]byte(iter.Value.(string)))
		if err != nil {
			t.Error("Error when producing a message.", err.Error())
		}
	}

	var messages list.List
	for {
		if data, err := dirq.ConsumeOne(); err != nil {
			t.Fatal(err)
		} else if data == nil {
			break
		} else {
			messages.PushBack(data)
		}
	}

	if messages.Len() != original.Len() {
		t.Errorf("Messages recovered do not match produced: %d != %d", messages.Len(), original.Len())
	}
}

// Setup
func TestMain(m *testing.M) {
	os.RemoveAll(dirqPath)
	os.Exit(m.Run())
}
