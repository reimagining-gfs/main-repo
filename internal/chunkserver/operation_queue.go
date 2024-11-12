package chunkserver

import (
)

func NewOperationQueue() *OperationQueue {
    return &OperationQueue{
        notEmpty: make(chan struct{}, 1),
    }
}

func (q *OperationQueue) Push(op *Operation) {
    q.mu.Lock()
    q.queue = append(q.queue, op)
    q.mu.Unlock()
    
    select {
    case q.notEmpty <- struct{}{}:
    default:
    }
}

func (q *OperationQueue) Pop() *Operation {
    q.mu.Lock()
    if len(q.queue) == 0 {
        q.mu.Unlock()
        <-q.notEmpty
        q.mu.Lock()
    }
    
    if len(q.queue) == 0 {
        q.mu.Unlock()
        return nil
    }
    
    op := q.queue[0]
    q.queue = q.queue[1:]
    q.mu.Unlock()
    return op
}

func (q *OperationQueue) Len() int {
    q.mu.Lock()
    defer q.mu.Unlock()
    return len(q.queue)
}
