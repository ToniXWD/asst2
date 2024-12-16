#include "tasksys.h"
#include <atomic>
#include <mutex>
#include <thread>
#include <vector>
#include <iostream>
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

    std::atomic<int> task_id(0);

    auto func = [&]() {
        while (true) {
            int cur_task_id = task_id.fetch_add(1);
            if (cur_task_id >= num_total_tasks) {
                return;
            }
            runnable->runTask(cur_task_id, num_total_tasks);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < max_threads; i++) {
        threads.push_back(std::thread(func));
    }

    for (auto& thread : threads) {
        thread.join();
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
        while (!this->is_terminate.load()) {
            if (this->is_finished.load()) {
                continue;
            }

            if (this->total_task_num == 0) {
                continue;
            }
            int  prev_left_task_num;
            if ((prev_left_task_num = this->left_task_num.fetch_sub(1)) > 0) {
                // 关键的原子操作，每个线程得到当前剩余的任务数量，从而获取一个任务索引
                auto cur_task_id = this->total_task_num - prev_left_task_num;
                this->runnable_ptr->runTask(cur_task_id, this->total_task_num);
                // std::cout << "task_id: " << cur_task_id << std::endl;
                if (prev_left_task_num == 1) {
                    this->is_finished.store(true);
                }
            } else {
                continue;
            }
        }
    };
    for (int i = 0; i < this->max_threads; i++) {
        this->worker_pool.push_back(std::thread(task_func));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // 等待所有线程完成
    this->is_terminate.store(true);
    for (auto& worker : worker_pool) {
        worker.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    this->total_task_num.store(num_total_tasks);  // 设置总任务数
    this->is_finished.store(false);

    this->runnable_ptr = runnable;
    this->left_task_num.store(num_total_tasks);

    // 如果 is_finished 为 true，则直接返回，否则死循环检查
    while (this->is_finished.load() != true) {
    }

    return;
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
        while (!this->is_terminate.load()) {
            if (this->total_task_num == 0) {
                // 如果任务还没有开始
                std::unique_lock<std::mutex> sl(this->start_lock);
                this->cv_start.wait(sl);
            }

            if (this->left_task_num.load() > 0 && this->runnable_ptr != nullptr) {
                IRunnable* runnable = this->runnable_ptr;
                auto prev_left_task_num = this->left_task_num.fetch_sub(1); // 关键的原子操作，每个线程得到当前剩余的任务数量，从而获取一个任务索引
                if (prev_left_task_num <= 0) {
                    continue;
                }
                auto cur_task_id = this->total_task_num - prev_left_task_num;
                runnable->runTask(cur_task_id, this->total_task_num);
                if (prev_left_task_num == 1) {

                    this->is_finished.store(true);
                    this->cv_success.notify_all();
                }
                continue;
            } else {
                // 等待任务重新开始
                std::unique_lock<std::mutex> ul(this->start_lock);
                this->cv_start.wait(ul);
            }
        }
    };
    for (int i = 0; i < this->max_threads; i++) {
        this->worker_pool.push_back(std::thread(task_func));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->is_terminate.store(true);
    this->cv_start.notify_all();
    for (auto& worker : worker_pool) {
        worker.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    this->total_task_num = num_total_tasks;  // 设置总任务数
    this->left_task_num.store(num_total_tasks);
    this->is_finished.store(false);               // 重置完成标志
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
