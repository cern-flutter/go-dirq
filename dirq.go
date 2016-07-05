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
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

type (
	// Dirq holds the configuration for a Directory Queue.
	Dirq struct {
		Path        string
		Umask       uint32
		MaxTempLife time.Duration
		MaxLockLife time.Duration
	}

	// Message wraps messages from Dirq. A message may carry an error.
	Message struct {
		Message []byte
		Error   error
	}
)

const (
	lockSuffix = ".lck"
	tempSuffix = ".tmp"
)

var (
	defaultUmask       = uint32(0022)
	defaultMaxTempLife = 300 * time.Second
	defaultMaxLockLife = 600 * time.Second
	directoryRegex     = regexp.MustCompile("^[0-9a-f]{8}$")
	fileRegex          = regexp.MustCompile("^[0-9a-f]{14}$")

	ErrDone = errors.New("Done consuming")
)

// newName generates a new name for a message
func generateName() string {
	now := time.Now()
	return fmt.Sprintf("%08x%05x%01x", now.Unix(), now.Nanosecond()/1000, rand.Int()%0xF)
}

// createDir creates a directory, but it does not fail if it exists
func createDir(dir string, umask uint32) error {
	if err := os.MkdirAll(dir, os.FileMode(0777&^umask)); err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

// New constructs a new DirQ handle.
func New(path string) (*Dirq, error) {
	if err := createDir(path, defaultUmask); err != nil {
		return nil, err
	}
	return &Dirq{
		Path:  path,
		Umask: defaultUmask,
	}, nil
}

// Close frees the memory associated with the dirq handle.
func (dirq *Dirq) Close() {
	// pass
}

// lock locks a file
func (dirq *Dirq) lock(file string) error {
	lockPath := file + lockSuffix
	if err := os.Link(file, lockPath); err != nil {
		return err
	}
	return nil
}

// remove removes both file and lock
func (dirq *Dirq) remove(file string) error {
	if err := os.Remove(file); err != nil {
		return err
	}
	if err := os.Remove(file + lockSuffix); err != nil {
		return err
	}
	return nil
}

// generateDirName returns a directory name based on time and granularity
func (dirq *Dirq) generateDirName() string {
	now := time.Now()
	return fmt.Sprintf("%08x", now.Unix())
}

// addData writes `data` into a file, returns the parent directory of the file, and the file full path
func (dirq *Dirq) addData(data []byte) (parent string, file string, err error) {
	parent = dirq.generateDirName()
	if err = createDir(path.Join(dirq.Path, parent), dirq.Umask); err != nil {
		return
	}

	file = path.Join(dirq.Path, parent, generateName()) + tempSuffix
	var fd *os.File
	if fd, err = os.OpenFile(file, os.O_WRONLY|os.O_CREATE, os.FileMode(0666&^dirq.Umask)); err != nil {
		return
	}

	if _, err = fd.Write(data); err != nil {
		fd.Close()
	} else {
		err = fd.Close()
	}
	return
}

// addPath creates a hardlink to the temporary file and removes the initial one.
func (dirq *Dirq) addPath(file, parent string) error {
	name := generateName()
	newPath := path.Join(dirq.Path, parent, name)
	if err := os.Link(file, newPath); err != nil {
		return err
	} else if err = os.Remove(file); err != nil {
		return err
	}
	return nil
}

// Produce a single message.
func (dirq *Dirq) Produce(data []byte) error {
	if parent, file, err := dirq.addData(data); err != nil {
		return err
	} else if err = dirq.addPath(file, parent); err != nil {
		return err
	}
	return nil
}

// walkFunc is called for each entry in the underlying dirq path
func (dirq *Dirq) consumeWalkFunc(file string, info os.FileInfo, err error, channel chan<- Message, justOne bool) error {
	if err != nil {
		channel <- Message{
			Error: err,
		}
		if justOne {
			return err
		}
		return nil
	}
	// Skip directory if the name does not match
	if info.IsDir() {
		if file == dirq.Path {
			return nil
		}
		if !directoryRegex.MatchString(info.Name()) {
			return filepath.SkipDir
		}
		return nil
	}
	// Process file
	if !fileRegex.MatchString(info.Name()) {
		return nil
	}

	if err = dirq.lock(file); err != nil {
		return err
	}
	defer dirq.remove(file)

	fd, err := os.Open(file)
	if err != nil {
		return err
	}
	defer fd.Close()

	data, err := ioutil.ReadAll(fd)
	if err != nil {
		return err
	}

	channel <- Message{
		Message: data,
	}

	if justOne {
		return ErrDone
	}
	return nil
}

// Consume messages on the DirQ directory. For long running processes,
// you may need to call this periodically, since the channel will be closed once it is out of
// messages, and you will lose any other coming in later.
func (dirq *Dirq) Consume() <-chan Message {
	channel := make(chan Message)
	go func() {
		defer close(channel)
		if err := filepath.Walk(dirq.Path, func(path string, info os.FileInfo, err error) error {
			return dirq.consumeWalkFunc(path, info, err, channel, false)
		}); err != nil {
			channel <- Message{Error: err}
		}
	}()
	return channel
}

// ConsumeOne consume just one message. It returns nil if empty
func (dirq *Dirq) ConsumeOne() ([]byte, error) {
	channel := make(chan Message, 1)

	if err := filepath.Walk(dirq.Path, func(path string, info os.FileInfo, err error) error {
		return dirq.consumeWalkFunc(path, info, err, channel, true)
	}); err != nil && err != ErrDone {
		return nil, err
	}
	close(channel)

	msg, ok := <-channel
	if ok {
		return msg.Message, nil
	}
	return nil, nil
}

// Empty returns true if there is nothing else in the queue
func (dirq *Dirq) Empty() (bool, error) {
	var err error
	if err = filepath.Walk(dirq.Path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Skip directory if the name does not match
		if info.IsDir() {
			if path == dirq.Path {
				return nil
			}
			if !directoryRegex.MatchString(info.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		// Process file
		if !fileRegex.MatchString(info.Name()) {
			return nil
		}
		// We got one!
		return ErrDone
	}); err != nil && err != ErrDone {
		return true, err
	}
	return err == nil, nil
}

// Purge cleans old directories and stale locks and temporary files.
func (dirq *Dirq) Purge() error {
	now := time.Now()
	return filepath.Walk(dirq.Path, func(path string, info os.FileInfo, err error) error {
		// Skip parent
		if path == dirq.Path {
			return nil
		}
		// If intermediate directory, try removing
		if info.IsDir() {
			if err := os.Remove(path); err == nil {
				return filepath.SkipDir
			} else if pathErr := err.(*os.PathError); pathErr.Err != syscall.ENOTEMPTY {
				return err
			}
			return nil
		}
		// If temporary file
		if strings.HasSuffix(info.Name(), tempSuffix) {
			if now.Sub(info.ModTime()) > dirq.MaxTempLife {
				return os.Remove(path)
			}
			return nil
		}
		// If lock
		if strings.HasSuffix(info.Name(), lockSuffix) {
			if now.Sub(info.ModTime()) > dirq.MaxLockLife {
				return os.Remove(path)
			}
			return nil
		}
		// Everything else
		return nil
	})
}
