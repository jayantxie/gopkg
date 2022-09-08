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

//go:build rpal
// +build rpal

package gopool

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync/atomic"

	"github.com/bytedance/gopkg/util/logger"
)

// defaultLazyWakePool is the global default lazy wake pool.
var defaultLazyWakePool LazyWakePool

func init() {
	defaultLazyWakePool = NewLazyWakePool("gopool.DefaultLazyWakePool", 10000, NewConfig())
}

func CtxGoLazyWake(ctx context.Context, f func()) {
	defaultLazyWakePool.CtxGoLazyWake(ctx, f)
}

type LazyWakePool interface {
	Pool
	// CtxGo executes f and accepts the context but lazy wake CtxGo
	CtxGoLazyWake(ctx context.Context, f func())
}

// NewLazyWakePool creates a new lazy wake pool with the given name, cap and config.
func NewLazyWakePool(name string, cap int32, config *Config) LazyWakePool {
	p := &pool{
		name:   name,
		cap:    cap,
		config: config,
	}
	return p
}

func (p *pool) CtxGoLazyWake(ctx context.Context, f func()) {
	t := taskPool.Get().(*task)
	t.ctx = ctx
	t.f = f
	p.taskLock.Lock()
	if p.taskHead == nil {
		p.taskHead = t
		p.taskTail = t
	} else {
		p.taskTail.next = t
		p.taskTail = t
	}
	p.taskLock.Unlock()
	atomic.AddInt32(&p.taskCount, 1)
	// The following two conditions are met:
	// 1. the number of tasks is greater than the threshold.
	// 2. The current number of workers is less than the upper limit p.cap.
	// or there are currently no workers.
	if (atomic.LoadInt32(&p.taskCount) >= p.config.ScaleThreshold && p.WorkerCount() < atomic.LoadInt32(&p.cap)) || p.WorkerCount() == 0 {
		p.incWorkerCount()
		w := workerPool.Get().(*worker)
		w.pool = p
		w.runLazyWake()
	}
}

func (w *worker) runLazyWake() {
	fn := func() {
		for {
			var t *task
			w.pool.taskLock.Lock()
			if w.pool.taskHead != nil {
				t = w.pool.taskHead
				w.pool.taskHead = w.pool.taskHead.next
				atomic.AddInt32(&w.pool.taskCount, -1)
			}
			if t == nil {
				// if there's no task to do, exit
				w.close()
				w.pool.taskLock.Unlock()
				w.Recycle()
				return
			}
			w.pool.taskLock.Unlock()
			func() {
				defer func() {
					if r := recover(); r != nil {
						msg := fmt.Sprintf("GOPOOL: panic in pool: %s: %v: %s", w.pool.name, r, debug.Stack())
						logger.CtxErrorf(t.ctx, msg)
						if w.pool.panicHandler != nil {
							w.pool.panicHandler(t.ctx, r)
						}
					}
				}()
				t.f()
			}()
			t.Recycle()
		}
	}
	runtime.NewProcLazyWake(fn)
}
