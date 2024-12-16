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

void TaskSystemParallelThreadPoolSpinning::worker() {
    while (!is_terminate) {
        mspin.lock();
        if (left_task_num <= 0) {
            mspin.unlock();
            continue;
        }
        int task_id = total_task_num - left_task_num;
        left_task_num--;
        mspin.unlock();
        runner->runTask(task_id, total_task_num);
        finished_task_num++;
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), max_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for (int i = 0; i < this->max_threads; i++) {
        this->worker_pool.push_back(std::thread([this]() {
            worker();
        }));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // 等待所有线程完成
    this->is_terminate = true;
    for (int i = 0; i < max_threads; ++i)
        worker_pool[i].join();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    finished_task_num = 0;
    this->total_task_num = num_total_tasks;
    runner = runnable;
    mspin.lock();
    left_task_num = num_total_tasks;
    mspin.unlock();
    while (finished_task_num < num_total_tasks) {
        continue;
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

void TaskSystemParallelThreadPoolSleeping::worker() {
    while (true) {
        std::unique_lock<std::mutex> lock(mtx_worker);
        cv_worker.wait(lock, [this] { return stop || left_task_num > 0; });

        if (stop && left_task_num == 0) {
            break;
        }

        int task_id = total_task_num - left_task_num;
        left_task_num--;
        lock.unlock();

        runner->runTask(task_id, total_task_num);

        {
            std::lock_guard<std::mutex> finish_lock(mtx_finish);
            finished_task_num++;
            if (finished_task_num == total_task_num) {
                cv_finish.notify_one();
            }
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    stop = false;
    total_task_num = left_task_num = 0;

    workers = new std::thread[num_threads];
    for (int i = 0; i < num_threads; ++i)
        workers[i] = std::thread([this]() { worker(); });
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->stop = true;
    cv_worker.notify_all();
    for (int i = 0; i < num_threads; ++i) {
        workers[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    runner = runnable;
    finished_task_num = 0;
    total_task_num = num_total_tasks;

    mtx_worker.lock();
    left_task_num = num_total_tasks;
    mtx_worker.unlock();

    cv_worker.notify_all();

    std::unique_lock<std::mutex> lock(mtx_finish);
    cv_finish.wait(lock, [&](){return finished_task_num == num_total_tasks;});
    lock.unlock();
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
