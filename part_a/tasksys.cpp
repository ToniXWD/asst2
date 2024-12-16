#include "tasksys.h"
#include <iostream>
#include <mutex>
#include <thread>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), max_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::vector<std::thread> threads;
    threads.reserve(this->max_threads);

    for (int task_id = 0; task_id < num_total_tasks; task_id++) {
        threads.emplace_back([runnable, task_id, num_total_tasks]() {
            runnable->runTask(task_id, num_total_tasks);
        });
        
        if ((task_id + 1) % this->max_threads == 0 || task_id == num_total_tasks - 1) {
            // 当线程数达到最大或是最后一批任务时
            for (auto& thread : threads) {
                thread.join();
            }
            threads.clear();
        }
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), max_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    auto task_func = [&]() {
        while (!this->is_finished) {
            if (this->left_task_num == 0) {
                continue;
            }
            if (this->left_task_num > 0 && this->runnable_ptr != nullptr) {
                IRunnable* runnable = this->runnable_ptr;
                auto prev_left_task_num = this->left_task_num.fetch_sub(1); // 关键的原子操作，每个线程得到当前剩余的任务数量，从而获取一个任务索引
                if (prev_left_task_num <= 0) {
                    this->is_finished = true;
                    return;
                }
                auto cur_task_id = this->total_task_num - prev_left_task_num;
                runnable->runTask(cur_task_id, this->total_task_num);
                if (prev_left_task_num == 1) {
                    this->is_finished = true;
                    return;
                }
                continue;
            }
        }
    };
    for (int i = 0; i < this->max_threads; i++) {
        this->worker_pool.emplace_back(task_func);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // 等待所有线程完成
    for (auto& worker : worker_pool) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    this->total_task_num = num_total_tasks;  // 设置总任务数
    this->is_finished = false;               // 重置完成标志
    this->runnable_ptr = runnable;
    this->left_task_num = num_total_tasks;

    while (!this->is_finished) {
        // std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), max_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    auto task_func = [&]() {
        while (!this->is_finished) {
            // 下面的 while 语句只会在任务开始时执行一次，因为之后的 total_task_num 会大于 0
            while (this->total_task_num == 0) {
                std::unique_lock<std::mutex> start_l(start_lock);
                this->cv_start.wait(start_l);
            }
            
            if (this->left_task_num > 0 && this->runnable_ptr != nullptr) {
                IRunnable* runnable = this->runnable_ptr;
                auto prev_left_task_num = this->left_task_num.fetch_sub(1); // 关键的原子操作，每个线程得到当前剩余的任务数量，从而获取一个任务索引
                if (prev_left_task_num <= 0) {
                    this->is_finished = true;
                    cv_start.notify_one();
                    return;
                }
                auto cur_task_id = this->total_task_num - prev_left_task_num;
                runnable->runTask(cur_task_id, this->total_task_num);
                if (prev_left_task_num == 1) {
                    this->is_finished = true;
                    cv_success.notify_one();
                    return;
                }
                continue;
            }
        }
    };

    for (int i = 0; i < this->max_threads; i++) {
        this->worker_pool.emplace_back(task_func);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for (auto& worker : worker_pool) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    this->total_task_num = num_total_tasks;  // 设置总任务数
    this->left_task_num = num_total_tasks;
    this->is_finished = false;               // 重置完成标志
    this->runnable_ptr = runnable;

    cv_start.notify_all();

    std::unique_lock<std::mutex> sl(this->success_lock);
    while (!this->is_finished) {
        this->cv_success.wait(sl);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
