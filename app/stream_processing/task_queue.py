import asyncio
import heapq
import time
from typing import Dict, List, Optional, Any, Callable, Set, Union
from dataclasses import dataclass, field
from enum import Enum
import structlog
import psutil
from datetime import datetime
import uuid


class TaskPriority(Enum):
    """Task priority levels"""
    CRITICAL = 0  # Real-time ticker updates
    HIGH = 10  # Orderbook updates
    MEDIUM = 20  # Klines, trades
    LOW = 30  # Historical data, cleanup


class WorkerType(Enum):
    """Types of workers for different task categories"""
    FAST = "fast"  # Ticker, critical updates (2-4 workers)
    MEDIUM = "medium"  # Orderbook, trades (2-3 workers)
    SLOW = "slow"  # Klines, historical (1-2 workers)


@dataclass
class Task:
    """Task item with priority and metadata"""
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = ""
    task_data: Dict[str, Any] = field(default_factory=dict)
    priority: int = TaskPriority.MEDIUM.value
    worker_type: WorkerType = WorkerType.MEDIUM
    created_at: float = field(default_factory=time.time)
    attempts: int = 0
    max_attempts: int = 3
    timeout: float = 30.0
    callback: Optional[Callable] = None

    def __lt__(self, other):
        """For heapq ordering by priority"""
        return self.priority < other.priority


class Worker:
    """Individual worker for processing tasks"""

    def __init__(self, worker_id: str, worker_type: WorkerType,
                 task_processor: Callable):
        self.worker_id = worker_id
        self.worker_type = worker_type
        self.task_processor = task_processor
        self.logger = structlog.get_logger().bind(
            component="worker", worker_id=worker_id, worker_type=worker_type.value
        )

        # State
        self.running = False
        self.current_task: Optional[Task] = None
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.total_processing_time = 0.0
        self.worker_task: Optional[asyncio.Task] = None

    async def start(self, task_queue: 'TaskQueue'):
        """Start worker"""
        self.running = True
        self.worker_task = asyncio.create_task(self._worker_loop(task_queue))
        self.logger.info("Worker started")

    async def stop(self):
        """Stop worker gracefully"""
        self.running = False

        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass

        self.logger.info("Worker stopped",
                         tasks_processed=self.tasks_processed,
                         tasks_failed=self.tasks_failed)

    async def _worker_loop(self, task_queue: 'TaskQueue'):
        """Main worker processing loop"""
        while self.running:
            try:
                # Get task from queue
                task = await task_queue.get_task(self.worker_type)

                if not task:
                    await asyncio.sleep(0.01)  # Brief pause if no tasks
                    continue

                # Process task
                await self._process_task(task)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Worker loop error", error=str(e))
                await asyncio.sleep(0.1)

    async def _process_task(self, task: Task):
        """Process individual task"""
        self.current_task = task
        start_time = time.time()

        try:
            self.logger.debug("Processing task",
                              task_id=task.task_id, task_type=task.task_type)

            # Process with timeout
            await asyncio.wait_for(
                self.task_processor(task),
                timeout=task.timeout
            )

            # Success metrics
            processing_time = time.time() - start_time
            self.tasks_processed += 1
            self.total_processing_time += processing_time

            # Call success callback
            if task.callback:
                await self._safe_callback(task.callback, task, None)

            self.logger.debug("Task completed",
                              task_id=task.task_id,
                              processing_time=processing_time)

        except asyncio.TimeoutError:
            self.logger.error("Task timeout",
                              task_id=task.task_id, timeout=task.timeout)
            await self._handle_task_failure(task, "timeout")

        except Exception as e:
            self.logger.error("Task processing failed",
                              task_id=task.task_id, error=str(e))
            await self._handle_task_failure(task, str(e))

        finally:
            self.current_task = None

    async def _handle_task_failure(self, task: Task, error: str):
        """Handle task failure with retry logic"""
        self.tasks_failed += 1
        task.attempts += 1

        # Call failure callback
        if task.callback:
            await self._safe_callback(task.callback, task, error)

        # Retry if attempts remaining
        if task.attempts < task.max_attempts:
            # Re-queue with lower priority (higher number)
            task.priority += 10
            # Will be handled by TaskQueue.add_task()

    async def _safe_callback(self, callback: Callable, task: Task, error: Optional[str]):
        """Safely execute task callback"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(task, error)
            else:
                callback(task, error)
        except Exception as e:
            self.logger.error("Callback failed",
                              task_id=task.task_id, error=str(e))

    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics"""
        avg_processing_time = (
            self.total_processing_time / self.tasks_processed
            if self.tasks_processed > 0 else 0
        )

        return {
            'worker_id': self.worker_id,
            'worker_type': self.worker_type.value,
            'running': self.running,
            'current_task': self.current_task.task_id if self.current_task else None,
            'tasks_processed': self.tasks_processed,
            'tasks_failed': self.tasks_failed,
            'success_rate': (
                (self.tasks_processed / (self.tasks_processed + self.tasks_failed)) * 100
                if (self.tasks_processed + self.tasks_failed) > 0 else 0
            ),
            'avg_processing_time': avg_processing_time
        }


class TaskQueue:
    """Priority-based task queue with dynamic worker management"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="task_queue")

        # Task queues by worker type
        self.task_queues: Dict[WorkerType, List[Task]] = {
            WorkerType.FAST: [],
            WorkerType.MEDIUM: [],
            WorkerType.SLOW: []
        }

        # Queue locks
        self.queue_locks: Dict[WorkerType, asyncio.Lock] = {
            worker_type: asyncio.Lock() for worker_type in WorkerType
        }

        # Workers
        self.workers: Dict[str, Worker] = {}
        self.worker_counts: Dict[WorkerType, int] = {
            WorkerType.FAST: 0,
            WorkerType.MEDIUM: 0,
            WorkerType.SLOW: 0
        }

        # Configuration
        self.worker_configs = {
            WorkerType.FAST: {'min': 2, 'max': 4, 'target_queue_size': 10},
            WorkerType.MEDIUM: {'min': 2, 'max': 3, 'target_queue_size': 20},
            WorkerType.SLOW: {'min': 1, 'max': 2, 'target_queue_size': 50}
        }

        # Task processors by type
        self.task_processors: Dict[str, Callable] = {}

        # State
        self.running = False
        self.scaling_task: Optional[asyncio.Task] = None

        # Metrics
        self.total_tasks_queued = 0
        self.total_tasks_completed = 0
        self.task_type_counts: Dict[str, int] = {}

    async def initialize(self):
        """Initialize task queue"""
        self.running = True

        # Start initial workers
        await self._scale_workers()

        # Start auto-scaling task
        self.scaling_task = asyncio.create_task(self._auto_scaling_loop())

        self.logger.info("Task queue initialized")

    async def shutdown(self):
        """Shutdown task queue gracefully"""
        self.running = False

        # Cancel scaling task
        if self.scaling_task:
            self.scaling_task.cancel()
            try:
                await self.scaling_task
            except asyncio.CancelledError:
                pass

        # Process remaining tasks with timeout
        await self._drain_queues(timeout=30.0)

        # Stop all workers
        worker_tasks = []
        for worker in self.workers.values():
            worker_tasks.append(worker.stop())

        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)

        self.workers.clear()
        self.logger.info("Task queue shutdown complete")

    def register_task_processor(self, task_type: str, processor: Callable):
        """Register task processor for specific task type"""
        self.task_processors[task_type] = processor
        self.logger.info("Task processor registered", task_type=task_type)

    async def add_task(self, task_type: str, task_data: Dict[str, Any],
                       priority: int = TaskPriority.MEDIUM.value,
                       worker_type: WorkerType = None,
                       timeout: float = 30.0,
                       callback: Optional[Callable] = None) -> str:
        """Add task to queue"""

        # Determine worker type based on task type if not specified
        if worker_type is None:
            worker_type = self._determine_worker_type(task_type)

        # Create task
        task = Task(
            task_type=task_type,
            task_data=task_data,
            priority=priority,
            worker_type=worker_type,
            timeout=timeout,
            callback=callback
        )

        # Add to appropriate queue
        async with self.queue_locks[worker_type]:
            heapq.heappush(self.task_queues[worker_type], task)

        # Update metrics
        self.total_tasks_queued += 1
        self.task_type_counts[task_type] = self.task_type_counts.get(task_type, 0) + 1

        self.logger.debug("Task queued",
                          task_id=task.task_id, task_type=task_type,
                          worker_type=worker_type.value, priority=priority)

        return task.task_id

    async def get_task(self, worker_type: WorkerType) -> Optional[Task]:
        """Get next task for worker type"""
        async with self.queue_locks[worker_type]:
            queue = self.task_queues[worker_type]

            if queue:
                return heapq.heappop(queue)

        return None

    def _determine_worker_type(self, task_type: str) -> WorkerType:
        """Determine appropriate worker type for task"""
        if 'ticker' in task_type or 'critical' in task_type:
            return WorkerType.FAST
        elif 'orderbook' in task_type or 'trades' in task_type:
            return WorkerType.MEDIUM
        else:
            return WorkerType.SLOW

    async def _auto_scaling_loop(self):
        """Auto-scaling loop for worker management"""
        while self.running:
            try:
                await asyncio.sleep(5.0)  # Check every 5 seconds
                await self._scale_workers()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Auto-scaling error", error=str(e))
                await asyncio.sleep(5.0)

    async def _scale_workers(self):
        """Scale workers based on queue sizes"""
        for worker_type in WorkerType:
            queue_size = len(self.task_queues[worker_type])
            current_workers = self.worker_counts[worker_type]
            config = self.worker_configs[worker_type]

            # Determine target worker count
            if queue_size > config['target_queue_size'] * 2:
                target_workers = min(config['max'], current_workers + 1)
            elif queue_size > config['target_queue_size']:
                target_workers = min(config['max'], current_workers)
            elif queue_size < config['target_queue_size'] // 2:
                target_workers = max(config['min'], current_workers - 1)
            else:
                target_workers = current_workers

            # Scale up
            while self.worker_counts[worker_type] < target_workers:
                await self._add_worker(worker_type)

            # Scale down
            while self.worker_counts[worker_type] > target_workers:
                await self._remove_worker(worker_type)

    async def _add_worker(self, worker_type: WorkerType):
        """Add new worker"""
        worker_id = f"{worker_type.value}_{self.worker_counts[worker_type]}"

        # Create task processor for this worker type
        task_processor = self._create_task_processor()

        # Create and start worker
        worker = Worker(worker_id, worker_type, task_processor)
        await worker.start(self)

        self.workers[worker_id] = worker
        self.worker_counts[worker_type] += 1

        self.logger.info("Worker added",
                         worker_id=worker_id, worker_type=worker_type.value)

    async def _remove_worker(self, worker_type: WorkerType):
        """Remove worker"""
        # Find worker to remove
        worker_to_remove = None
        for worker_id, worker in self.workers.items():
            if worker.worker_type == worker_type:
                worker_to_remove = worker
                break

        if worker_to_remove:
            await worker_to_remove.stop()
            del self.workers[worker_to_remove.worker_id]
            self.worker_counts[worker_type] -= 1

            self.logger.info("Worker removed",
                             worker_id=worker_to_remove.worker_id,
                             worker_type=worker_type.value)

    def _create_task_processor(self) -> Callable:
        """Create task processor function"""

        async def process_task(task: Task):
            # Get registered processor for task type
            if task.task_type in self.task_processors:
                processor = self.task_processors[task.task_type]
                await processor(task.task_data)
            else:
                # Default processor
                await self._default_task_processor(task)

        return process_task

    async def _default_task_processor(self, task: Task):
        """Default task processor"""
        self.logger.warning("No processor registered for task type",
                            task_type=task.task_type)
        # Could implement basic database write here
        await asyncio.sleep(0.01)  # Simulate processing

    async def _drain_queues(self, timeout: float = 30.0):
        """Drain remaining tasks from queues"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            total_tasks = sum(len(queue) for queue in self.task_queues.values())

            if total_tasks == 0:
                break

            await asyncio.sleep(0.1)

        # Log remaining tasks
        remaining_tasks = sum(len(queue) for queue in self.task_queues.values())
        if remaining_tasks > 0:
            self.logger.warning("Tasks remaining after drain",
                                remaining_tasks=remaining_tasks)

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive queue statistics"""
        queue_stats = {}
        worker_stats = {}

        # Queue statistics
        for worker_type in WorkerType:
            queue_stats[worker_type.value] = {
                'queue_size': len(self.task_queues[worker_type]),
                'worker_count': self.worker_counts[worker_type],
                'config': self.worker_configs[worker_type]
            }

        # Worker statistics
        for worker_id, worker in self.workers.items():
            worker_stats[worker_id] = worker.get_stats()

        # Memory usage
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024

        return {
            'total_tasks_queued': self.total_tasks_queued,
            'total_tasks_completed': self.total_tasks_completed,
            'task_type_counts': dict(self.task_type_counts),
            'queue_stats': queue_stats,
            'worker_stats': worker_stats,
            'memory_usage_mb': memory_mb,
            'running': self.running
        }


# Global instance
task_queue = TaskQueue()
