#include "MapReduceFramework.h"
#include "Barrier.h"
#include <mutex>
#include <atomic>
#include <thread>
#include <vector>
#include <iostream>
#include <atomic>
#include <cstdint>


using IntermediateVec = std::vector<std::pair<K2*,V2*>>;
struct ThreadContext;
struct JobContext;

void mapWorker(ThreadContext* context);
void shuffle(JobContext* jobContext);
void reduceWorker(ThreadContext* context);
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


struct ThreadContext {
/// This struct is used to pass context to the worker threads.
    JobContext* job; // Pointer to the job context
    int thread_id;   // Thread ID
    IntermediateVec intermediateVec; // Vector for intermediate key-value pairs
    std::mutex mutex; // Mutex for thread safety
};

struct JobContext {
// each job goes through 3/4 stages so we need to keep all its info together
// used internally to manage the job.
    const MapReduceClient* client; // Pointer to the client
    Barrier barrier; // to wait until shuffling is done
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
    std::atomic<int> totalIntermediaryPairs;
    std::atomic<int> totalOutputPairs;
    std::vector<ThreadContext> threadContexts;

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

JobContext(const MapReduceClient* client, const InputVec& inputVec, OutputVec* outputVec, int multiThreadLevel)
    : client(client),
      barrier(multiThreadLevel),
      inputVec(std::move(inputVec)),
      outputVec(outputVec),
      next_input_index(0),
      joined(false),
      is_deleted(false),
      totalIntermediaryPairs(0),
      totalOutputPairs(0),
      threadContexts(multiThreadLevel)
    {
    // Initialize the job state
    setJobState(stage_t::MAP_STAGE, 0, this->inputVec.size());
    // Initialize intermediateVecsForReduce
    intermediateVecsForReduce.resize(multiThreadLevel);
}
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
    if (job == nullptr) {
        return; // No job to close
    }
    // Make sure that all threads in MapReduce job finish execution before moving on.
    auto* jobContext = static_cast<JobContext*>(job);
    std::unique_lock<std::mutex> lock(jobContext->delete_mutex);
    if (jobContext->is_deleted) {
        return; // Already deleted
    }
    waitForJob(job); // Ensure the job is finished before cleanup
    jobContext->is_deleted = true; // Mark as deleted
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
    threadContext->intermediateVec.emplace_back(std::move(key), std::move(value));

    // update the number of intermediary elements using atomic counter
    threadContext->job->totalIntermediaryPairs.fetch_add(1); // increment number of emitted pairs

}

void emit3(K3* key, V3* value, void* context) {
    // The user calls this function in the reduce phase to emit output key-value pairs
    auto* threadContext = static_cast<ThreadContext*>(context);
    std::lock_guard<std::mutex> lock(threadContext->job->output_mutex); // Mutex is now per-job

    threadContext->job->outputVec->emplace_back(std::move(key), std::move(value));
    threadContext->job->totalOutputPairs.fetch_add(1); // increment number of emitted pairs
}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    // Allocate and initialize JobContext
    auto* jobContext = new JobContext(&client, inputVec, &outputVec, multiThreadLevel);
    if (!jobContext) {
        std::cerr << "Error: Failed to allocate memory for JobContext." << std::endl;
        return nullptr;
    }

    // Initialize threadContexts and create threads
    for (std::size_t i = 0; i < multiThreadLevel; ++i) {
        if (i >= jobContext->threadContexts.size()) {
            std::cerr << "Error: Invalid thread context index." << std::endl;
            delete jobContext;
            return nullptr;
        }
        jobContext->threadContexts[i].job = jobContext;
        jobContext->threadContexts[i].thread_id = i;

        try {
            jobContext->threads.emplace_back(mapWorker, &jobContext->threadContexts[i]);
        } catch (const std::system_error& e) {
            std::cerr << "Error: Failed to create thread " << i << ": " << e.what() << std::endl;
            waitForJob(static_cast<JobHandle>(jobContext));
            delete jobContext;
            return nullptr;
        }
    }
    return static_cast<JobHandle>(jobContext);
}

//
//void mapWorker(ThreadContext* threadContext) {
//    // Stage 1: Each thread processes its own input pairs
//    int index = threadContext->job->next_input_index.fetch_add(1); // Use next_input_index
//    while (index < threadContext->job->inputVec.size()) {
//        K1* key = threadContext->job->inputVec[index].first;
//        V1* value = threadContext->job->inputVec[index].second;
//        try {
//            threadContext->job->client->map(key, value, threadContext);
//        } catch (const std::exception& e) {
//            threadContext->job->setJobState(stage_t::UNDEFINED_STAGE, 0, 0);
//            std::cerr << "Error in map function: " << e.what() << std::endl;
//            return;
//        }
//        int processed = index + 1;
//        threadContext->job->setJobState(stage_t::MAP_STAGE, processed, threadContext->job->inputVec.size());
//        index = threadContext->job->next_input_index.fetch_add(1);
//    }
//    // sorting the intermediate vector
//    std::sort(threadContext->intermediateVec.begin(), threadContext->intermediateVec.end(),
//              [](const auto& a, const auto& b) {
//                  return *(a.first) < *(b.first);
//              });
//    {
//    std::lock_guard<std::mutex> lock(threadContext->job->output_mutex);
//    threadContext->job->intermediateVecsForReduce[ threadContext->thread_id ] = std::move(threadContext->intermediateVec);
//    }
//
//    // waiting for all threads to finish sorting
//    threadContext->job->barrier.barrier();
//    // only the first thread will shuffle
//    if (threadContext->thread_id == 0) {
//        uint32_t processed = threadContext->job->totalIntermediaryPairs.load();
//        uint32_t expected = threadContext->job->inputVec.size();
//        if (processed != expected) {
//            std::cerr << "Warning: MAP_STAGE ended with mismatch. Processed = "
//                    << processed << ", Expected = " << expected << std::endl;
//        }
//        threadContext->job->setJobState(stage_t::SHUFFLE_STAGE, 0, processed);
//        shuffle(threadContext->job);
//    }
//    // waiting for all threads to finish shuffling
//    threadContext->job->barrier.barrier();
//    threadContext->job->atomicState.updateStage(stage_t::REDUCE_STAGE);
//    reduceWorker(threadContext);
//}


void mapWorker(ThreadContext* threadContext) {
    // Stage 1: Each thread processes its own input pairs
    int index = threadContext->job->next_input_index.fetch_add(1); // Use next_input_index
    while (index < threadContext->job->inputVec.size()) {
        K1* key = threadContext->job->inputVec[index].first;
        V1* value = threadContext->job->inputVec[index].second;
        try {
            threadContext->job->client->map(key, value, threadContext);
        } catch (const std::exception& e) {
            threadContext->job->setJobState(stage_t::UNDEFINED_STAGE, 0, 0);
            std::cerr << "Error in map function: " << e.what() << std::endl;
            return;
        }
        int processed = index + 1;
        threadContext->job->setJobState(stage_t::MAP_STAGE, processed, threadContext->job->inputVec.size());
        index = threadContext->job->next_input_index.fetch_add(1);
    }
    // sorting the intermediate vector
    std::sort(threadContext->intermediateVec.begin(), threadContext->intermediateVec.end(),
              [](const auto& a, const auto& b) {
                  return *(a.first) < *(b.first);
              });
    {
    std::lock_guard<std::mutex> lock(threadContext->job->output_mutex);
    threadContext->job->intermediateVecsForReduce[ threadContext->thread_id ] = std::move(threadContext->intermediateVec);
    }

    // waiting for all threads to finish sorting
    threadContext->job->barrier.barrier();
    // only the first thread will shuffle
    if (threadContext->thread_id == 0) {
        uint32_t sum = 0;
        for (auto &vec : threadContext->job->intermediateVecsForReduce) {
            sum += vec.size();
        }
        uint32_t total = threadContext->job->totalIntermediaryPairs.load();
        if (sum != total) {
            std::cerr << "Sanity check FAILED in MAP_STAGE: "
                  << "sum(vec sizes)=" << sum
                  << ", totalIntermediaryPairs=" << total
                  << std::endl;
    }
        threadContext->job->setJobState(stage_t::SHUFFLE_STAGE, 0, total);
        shuffle(threadContext->job);
    }
    // waiting for all threads to finish shuffling
    threadContext->job->barrier.barrier();
    threadContext->job->atomicState.updateStage(stage_t::REDUCE_STAGE);
    reduceWorker(threadContext);
}

void shuffle(JobContext* jobContext) {
    // Queue to store vectors of identical keys
    std::vector<IntermediateVec> shuffledQueue;
    std::atomic<int> shuffledCount(0);
    IntermediateVec currentVec; ////// changed this!!!!!!

    // While there are still elements in the intermediate vectors
    while (true) {
        bool allEmpty = true;

        // Iterate over each thread's intermediate vector
        for (auto& intermediateVec : jobContext->intermediateVecsForReduce) {
            if (intermediateVec.empty()) {
                continue;
            }

            allEmpty = false;

            // Take the last pair from the vector
            auto& lastPair = intermediateVec.back();
            K2* key = lastPair.first;
            V2* value = lastPair.second;

            // If the current vector is empty or the key changes, push the current vector to the queue
            if (!currentVec.empty() && (*(currentVec.back().first) < *key || *key < *(currentVec.back().first))) {
                shuffledQueue.push_back(std::move(currentVec));
                shuffledCount.fetch_add(1);
                jobContext->setJobState(stage_t::SHUFFLE_STAGE, shuffledCount.load(), jobContext->totalIntermediaryPairs.load());
                currentVec.clear();
            }

            // Add the pair to the current vector
            currentVec.emplace_back(key, value);
            intermediateVec.pop_back();
        }

        // If all vectors are empty, push the last vector and break
        if (allEmpty) {
            if (!currentVec.empty()) {
                shuffledQueue.push_back(std::move(currentVec));
                shuffledCount.fetch_add(1);
            }
            break;
        }
    }

    // Update the intermediate vectors for reduce
    jobContext->intermediateVecsForReduce = std::move(shuffledQueue);
}

void reduceWorker(ThreadContext* threadContext) {
    auto* jobContext = threadContext->job;

    // שלב 0: שמור מראש את מספר הוקטורים לעיבוד (בצורה מסונכרנת)
    size_t expectedTotal;
    {
        std::lock_guard<std::mutex> lock(jobContext->output_mutex);
        expectedTotal = jobContext->intermediateVecsForReduce.size();
    }

    jobContext->setJobState(stage_t::REDUCE_STAGE, 0, expectedTotal);

    while (true) {
        IntermediateVec currentVec;
        {
            std::lock_guard<std::mutex> lock(jobContext->output_mutex);
            if (jobContext->intermediateVecsForReduce.empty()) {
                break; 
            }

            currentVec = std::move(jobContext->intermediateVecsForReduce.back());
            jobContext->intermediateVecsForReduce.pop_back();
        }
        try {
            jobContext->client->reduce(&currentVec, threadContext);
        } catch (const std::exception& e) {
            std::cerr << "Error in reduce function: " << e.what() << std::endl;
            return;
        }
        uint32_t processed = jobContext->totalOutputPairs.load();  // instead of incrementing again
        jobContext->setJobState(stage_t::REDUCE_STAGE, processed, expectedTotal);
    }

    jobContext->setJobState(stage_t::REDUCE_STAGE, jobContext->totalOutputPairs.load(), expectedTotal);
}

