// Copyright 2021 ByteDance Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gopool

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/bytedance/gopkg/util/logger"
)

var workerPool sync.Pool

func init() {
	workerPool.New = newWorker
}

type worker struct {
	pool *pool
}

func newWorker() interface{} {
	return &worker{}
}

func (w *worker) run() {
	go func() {
		var spinTimes int32
		var enterSpin bool
		for {
			var t *task
			if atomic.LoadInt32(&w.pool.taskCount) > 0 {
				w.pool.taskLock.Lock()
				if w.pool.taskHead != nil {
					t = w.pool.taskHead
					w.pool.taskHead = w.pool.taskHead.next
					atomic.AddInt32(&w.pool.taskCount, -1)
				}
				w.pool.taskLock.Unlock()
			}
			if t == nil {
				// if there's no task to do, spin for MaxSpinTimes times and then close if still no task.
				if spinTimes < w.pool.config.MaxSpinTimes {
					if spinTimes == 0 && w.shouldEnterSpin() {
						enterSpin = true
					}
					if enterSpin {
						spinTimes++
						runtime.Gosched()
						continue
					}
				}
				if enterSpin {
					atomic.AddInt32(&w.pool.spinWorkerCount, -1)
				}
				w.close()
				w.Recycle()
				return
			}
			if enterSpin {
				atomic.AddInt32(&w.pool.spinWorkerCount, -1)
			}
			spinTimes = 0
			enterSpin = false
			func() {
				defer func() {
					if r := recover(); r != nil {
						if w.pool.panicHandler != nil {
							w.pool.panicHandler(t.ctx, r)
						} else {
							msg := fmt.Sprintf("GOPOOL: panic in pool: %s: %v: %s", w.pool.name, r, debug.Stack())
							logger.CtxErrorf(t.ctx, msg)
						}
					}
				}()
				t.f()
			}()
			t.Recycle()
		}
	}()
}

func (w *worker) shouldEnterSpin() bool {
	//if current := atomic.LoadInt32(&w.pool.spinWorkerCount); current+1 <= w.pool.config.MaxSpinWorkers {
	//	if atomic.CompareAndSwapInt32(&w.pool.spinWorkerCount, current, current+1) {
	//		return true
	//	}
	//}
	//return false
	return atomic.CompareAndSwapInt32(&w.pool.spinWorkerCount, 0, 1)
}

func (w *worker) close() {
	w.pool.decWorkerCount()
}

func (w *worker) zero() {
	w.pool = nil
}

func (w *worker) Recycle() {
	w.zero()
	workerPool.Put(w)
}
