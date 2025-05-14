#include "MapReduceFramework.h"
#include "Barrier.h"
#include <mutex>
#include <atomic>
#include <thread>
#include <vector>
#include <iostream>
#include <atomic>
#include <cstdint>

enum stage_t {
    UNDEFINED_STAGE = 0,
    MAP_STAGE = 1,
    SHUFFLE_STAGE = 2,
    REDUCE_STAGE = 3
};

void mapworker(ThreadContext* context);

class JobStateAtomic {
/// This class is used to manage the state of a job in a thread-safe manner.
/// It uses atomic operations to ensure stage, processed and total
/// counts are changed without race conditions.
private:
    std::atomic<uint64_t> packed;  // Stores everything

public:
    JobStateAtomic() : packed(0) {} // constructor initializes packed to 0

    // Set stage, processed count, total count
    void set(stage_t stage, uint32_t processed, uint32_t total) {
        uint64_t value = 0;
        value |= static_cast<uint64_t>(stage);            // bits 0–1
        value |= static_cast<uint64_t>(processed) << 2;   // bits 2–32
        value |= static_cast<uint64_t>(total) << 33;      // bits 33–63
        packed.store(value);
    }

    // Read current stage and percentage
    void get(stage_t& stage, float& percentage) {
        uint64_t value = packed.load();
        stage = static_cast<stage_t>(value & 0b11);             // bits 0–1
        uint32_t processed = (value >> 2) & 0x7FFFFFFF;         // bits 2–32
        uint32_t total = (value >> 33) & 0x7FFFFFFF;            // bits 33–63

        percentage = (total == 0) ? 0.0f : (100.0f * processed / total);
        // If total is 0, percentage is set to 0.0f to avoid division by zero
    }

    // Update how many items were processed (stage & total stay the same)
    void updateProcessed(uint32_t processed) {
        stage_t stage; // not used here
        float percent; // not used here
        uint64_t value = packed.load();
        uint32_t total = (value >> 33) & 0x7FFFFFFF;
        get(stage, percent);
        set(stage, processed, total); // repack with new processed value
    }

    // Update the stage (processed & total stay the same)
    void updateStage(stage_t new_stage) {
        uint64_t value = packed.load();  // Load the current packed value
        uint32_t processed = (value >> 2) & 0x7FFFFFFF; // extract processed
        uint32_t total = (value >> 33) & 0x7FFFFFFF; // extract total
        set(new_stage, processed, total);
    }
};

struct JobContext {
// each job goes through 3/4 stages so we need to keep all its info together
// used internally to manage the job.
    const MapReduceClient* client; // Pointer to the client
    Barrier barrier; // to wait untill sort
    std::vector<std::thread> threads;
    std::vector<IntermediateVec> intermediateVecsForReduce; // vector of vectors - make sure it contains a sequence of pairs (k2, v2) where all keys are identical
    std::mutex join_mutex;      // Protects joining logic
    bool joined = false;        // Indicates if threads were already joined (indicates if the job is finished)
    bool is_deleted = false;  // Indicates if the job context is deleted
    std::mutex delete_mutex;  // Protects deletion logic
    InputVec inputVec;
    OutputVec* outputVec;
    std::atomic<int> next_input_index; // ensures each thread has unique input pair
    JobStateAtomic atomicState; // Manages state atomically
    std::mutex output_mutex;
    std::atomic<int> totalIntermediaryPairs = 0;
    std::atomic<int> totalOutputPairs = 0;
    std::vector<ThreadContext>* threadContexts;

    // Helper to get the current JobState
    JobState getJobState() {
        JobState state;
        atomicState.get(state.stage, state.percentage);
        return state;
    }

    // Helper to update the JobState
    void setJobState(stage_t stage, uint32_t processed, uint32_t total) {
        atomicState.set(stage, processed, total);
    }

        // Constructor
        JobContext(const MapReduceClient* client, const InputVec& inputVec,  OutputVec* outputVec, int multiThreadLevel): 
        client(client),
        barrier(multiThreadLevel),
        inputVec(inputVec),
        outputVec(outputVec),
        next_input_index(0),
        joined(false),
        is_deleted(false),
        totalIntermediaryPairs(0),
        totalOutputPairs(0),
        threadContexts(nullptr) {atomicState.set(MAP_STAGE, 0, inputVec.size());}
};

struct JobState {
// used to communicate the job's state to external functions or users
    stage_t stage = static_cast<stage_t>{UNDEFINED_STAGE}; // always start with UNDEFINED_STAGE
    float percentage = 0.0f; // percentage of completion (0-100)
};

struct ThreadContext {
/// This struct is used to pass context to the worker threads.
    JobContext* job; // Pointer to the job context
    int thread_id;   // Thread ID
    IntermediateVec intermediateVec; // Vector for intermediate key-value pairs
    std::mutex mutex; // Mutex for thread safety
};

void waitForJob(JobHandle job) {
// Make sure that all threads in MapReduce job finish execution before moving on.
// usually called by the main thread after starting the job.
    auto* jobContext = static_cast<JobContext*>(job); // Cast job to JobContext
    std::unique_lock<std::mutex> lock(jobContext->join_mutex); // Lock the mutex to ensure thread safety (automatically unlocks when going out of scope)
    if (jobContext->joined) {
        return; // Already joined
    }
    for (auto& thread : jobContext->threads) {
        if (thread.joinable()) {
            thread.join(); // Wait for the thread to finish
        }
    }
    jobContext->joined = true; // Mark as joined
}


void closeJobHandle(JobHandle job) {
    auto* jobContext = static_cast<JobContext*>(job);
    std::unique_lock<std::mutex> lock(jobContext->delete_mutex);
    if (jobContext->is_deleted) {
        return; // Already deleted
    }
    waitForJob(job); // Ensure the job is finished before cleanup
    jobContext->is_deleted = true; // Mark as deleted
    delete jobContext->threadContexts; // Delete the threadContexts vector
    delete jobContext; // Delete the JobContext
}

void getJobState(JobHandle job, JobState* state) {
    auto* jobContext = static_cast<JobContext*>(job);
    jobContext->atomicState.get(state->stage, state->percentage);
    // extract the current stage atomically + compute the percentage
    // write these values into state passed by the user
}


void emit2(K2* key, V2* value, void* context) {
// the user calls this function while in map phase to emit intermediate key-value pairs
// receives a pair k2,v2 and saves it in the intermediate vector given in context
    auto* threadContext = static_cast<ThreadContext*>(context); // assume context is a ThreadContext*
    threadContext->intermediateVec.emplace_back(key, value); // add the pair to the intermediate vector

    // update the number of intermediary elements using atomic counter
    threadContext->job->totalIntermediaryPairs.fetch_add(1); // increment number of emitted pairs

}

void emit3(K3* key, V3* value, void* context) {
    // The user calls this function in the reduce phase to emit output key-value pairs
    auto* threadContext = static_cast<ThreadContext*>(context);
    std::lock_guard<std::mutex> lock(threadContext->job->output_mutex); // Mutex is now per-job

    threadContext->job->outputVec->emplace_back(key, value);
    threadContext->job->totalOutputPairs.fetch_add(1); // increment number of emitted pairs
}


JobHandle startMapReduceJob(const MapReduceClient& client,
    const InputVec& inputVec, OutputVec& outputVec,
    int multiThreadLevel) {
auto* jobContext = new JobContext(&client, inputVec, &outputVec, multiThreadLevel);
if (!jobContext) {
    exit(1); // change to the right error
}
auto* threadContexts = new std::vector<ThreadContext>(multiThreadLevel);
if (!threadContexts) {
    exit(1); // change to the right error
}
jobContext->threadContexts = threadContexts;

for (int i = 0; i < multiThreadLevel; ++i) {
(*threadContexts)[i].job = jobContext;
(*threadContexts)[i].thread_id = i;
jobContext->threads.emplace_back(mapworker, &(*threadContexts)[i]);
}

return static_cast<JobHandle>(jobContext);
}

void mapworker(ThreadContext* context) {
    JobContext* job = context->job;
    int index;

    while ((index = job->next_input_index.fetch_add(1)) < job->inputVec.size()) {
        const auto& pair = job->inputVec[index];
        job->client->map(pair.first, pair.second, context);

        job->atomicState.updateProcessed(index + 1);
    }

    std::sort(context->intermediateVec.begin(), context->intermediateVec.end(),
        [](const IntermediatePair& a, const IntermediatePair& b) {
            return *a.first < *b.first;
        });

    job->barrier.barrier();
}


//// what do we still need to do?
// 1. Implement the sort and shuffle functions (operator < given by client(?))
// 4. Implement the mapworker
// 5. Implement the reduceWorker
// (in 4 and 5 we save the old_value and give it to the cur thread)


