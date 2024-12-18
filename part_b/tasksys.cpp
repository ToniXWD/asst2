#include "tasksys.h"
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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
        auto wait_func = [this] { return this->stop || this->left_task_num > 0; };
        cv_worker.wait(lock, wait_func);

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

    for (int i = 0; i < this->num_threads; ++i) {
        workers.push_back(std::thread([this]() { worker(); }));
    }
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
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join(); // 等待所有线程完成
        }
    }

    // 释放所有任务上下文
    for (auto& pair : task_contexts) {
        delete pair.second;
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

    {
        std::lock_guard<std::mutex> lock(mtx_worker);
        left_task_num = num_total_tasks;
    }

    cv_worker.notify_all();

    std::unique_lock<std::mutex> lock(mtx_finish);
    auto wait_func = [this](){return this->finished_task_num == this->total_task_num;};
    cv_finish.wait(lock, wait_func);
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // 先等待依赖任务完成
    for (const auto& dep : deps) {
        TaskContext* task_context = this->task_contexts[dep];
        std::unique_lock<std::mutex> lock(task_context->mtx);
        task_context->cv.wait(lock, [task_context]() { return task_context->is_finished; });
    }

    int cur_task_id = this->next_task_id.fetch_add(1);
    TaskContext* task_context = new TaskContext(cur_task_id);

    // 在另一个线程中执行任务 TaskSystemParallelThreadPoolSleeping::run 函数
    std::thread([this, runnable, num_total_tasks, task_context]() {
        this->run(runnable, num_total_tasks);
        {
            std::lock_guard<std::mutex> lock(task_context->mtx);
            task_context->is_finished = true;
        }
        task_context->cv.notify_all();
    }).detach();

    this->task_contexts[cur_task_id] = task_context;
    
    return cur_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    for (const auto& pair : this->task_contexts) {
        TaskContext* task_context = pair.second;

        std::unique_lock<std::mutex> lock(task_context->mtx);
        task_context->cv.wait(lock, [task_context]() { return task_context->is_finished; });
    }

    return;
}
