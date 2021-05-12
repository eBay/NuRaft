/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright:
See URL: https://github.com/datatechnology/cornerstone

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#ifndef _STATE_MACHINE_HXX_
#define _STATE_MACHINE_HXX_

#include "async.hxx"
#include "basic_types.hxx"
#include "buffer.hxx"
#include "pp_util.hxx"
#include "ptr.hxx"

namespace nuraft {

class cluster_config;
class snapshot;
class state_machine {
    __interface_body__(state_machine);

public:
    struct ext_op_params {
        ext_op_params(ulong _log_idx,
                      ptr<buffer>& _data)
            : log_idx(_log_idx)
            , data(_data)
            {}
        ulong log_idx;
        ptr<buffer>& data;
        // May add more parameters in the future.
    };

    /**
     * Commit the given Raft log.
     *
     * NOTE:
     *   Given memory buffer is owned by caller, so that
     *   commit implementation should clone it if user wants to
     *   use the memory even after the commit call returns.
     *
     *   Here provide a default implementation for facilitating the
     *   situation when application does not care its implementation.
     *
     * @param log_idx Raft log number to commit.
     * @param data Payload of the Raft log.
     * @return Result value of state machine.
     */
    virtual ptr<buffer> commit(const ulong log_idx,
                               buffer& data) { return nullptr; }

    /**
     * (Optional)
     * Extended version of `commit`, for users want to keep
     * the data without any extra memory copy.
     */
    virtual ptr<buffer> commit_ext(const ext_op_params& params)
    {   return commit(params.log_idx, *params.data);    }

    /**
     * (Optional)
     * Handler on the commit of a configuration change.
     *
     * @param log_idx Raft log number of the configuration change.
     * @param new_conf New cluster configuration.
     */
    virtual void commit_config(const ulong log_idx, ptr<cluster_config>& new_conf) { }

    /**
     * Pre-commit the given Raft log.
     *
     * Pre-commit is called after appending Raft log,
     * before getting acks from quorum nodes.
     * Users can ignore this function if not needed.
     *
     * Same as `commit()`, memory buffer is owned by caller.
     *
     * @param log_idx Raft log number to commit.
     * @param data Payload of the Raft log.
     * @return Result value of state machine.
     */
    virtual ptr<buffer> pre_commit(const ulong log_idx,
                                   buffer& data) { return nullptr; }

    /**
     * (Optional)
     * Extended version of `pre_commit`, for users want to keep
     * the data without any extra memory copy.
     */
    virtual ptr<buffer> pre_commit_ext(const ext_op_params& params)
    {   return pre_commit(params.log_idx, *params.data);  }

    /**
     * Rollback the state machine to given Raft log number.
     *
     * It will be called for uncommitted Raft logs only,
     * so that users can ignore this function if they don't
     * do anything on pre-commit.
     *
     * Same as `commit()`, memory buffer is owned by caller.
     *
     * @param log_idx Raft log number to commit.
     * @param data Payload of the Raft log.
     */
    virtual void rollback(const ulong log_idx,
                          buffer& data) {}

    /**
     * (Optional)
     * Extended version of `rollback`, for users want to keep
     * the data without any extra memory copy.
     */
    virtual void rollback_ext(const ext_op_params& params)
    {   rollback(params.log_idx, *params.data);  }

    /**
     * (Optional)
     * Return a hint about the preferred size (in number of bytes)
     * of the next batch of logs to be sent from the leader.
     *
     * Only applicable on followers.
     *
     * @return The preferred size of the next log batch.
     *         `0` indicates no preferred size (any size is good).
     *         `positive value` indicates at least one log can be sent,
     *         (the size of that log may be bigger than this hint size).
     *         `negative value` indicates no log should be sent since this
     *         follower is busy handling pending logs.
     */
    virtual int64 get_next_batch_size_hint_in_bytes() { return 0; }

    /**
     * (Deprecated)
     * Save the given snapshot chunk to local snapshot.
     * This API is for snapshot receiver (i.e., follower).
     *
     * Since snapshot itself may be quite big, save_snapshot_data()
     * will be invoked multiple times for the same snapshot `s`. This
     * function should decode the {offset, data} and re-construct the
     * snapshot. After all savings are done, apply_snapshot() will be
     * called at the end.
     *
     * Same as `commit()`, memory buffer is owned by caller.
     *
     * @param s Snapshot instance to save.
     * @param offset Byte offset of given chunk.
     * @param data Payload of given chunk.
     */
    virtual void save_snapshot_data(snapshot& s,
                                    const ulong offset,
                                    buffer& data) {}

    /**
     * Save the given snapshot object to local snapshot.
     * This API is for snapshot receiver (i.e., follower).
     *
     * This is an optional API for users who want to use logical
     * snapshot. Instead of splitting a snapshot into multiple
     * physical chunks, this API uses logical objects corresponding
     * to a unique object ID. Users are responsible for defining
     * what object is: it can be a key-value pair, a set of
     * key-value pairs, or whatever.
     *
     * Same as `commit()`, memory buffer is owned by caller.
     *
     * @param s Snapshot instance to save.
     * @param obj_id[in,out]
     *     Object ID.
     *     As a result of this API call, the next object ID
     *     that reciever wants to get should be set to
     *     this parameter.
     * @param data Payload of given object.
     * @param is_first_obj `true` if this is the first object.
     * @param is_last_obj `true` if this is the last object.
     */
    virtual void save_logical_snp_obj(snapshot& s,
                                      ulong& obj_id,
                                      buffer& data,
                                      bool is_first_obj,
                                      bool is_last_obj) {}

    /**
     * Apply received snapshot to state machine.
     *
     * @param s Snapshot instance to apply.
     * @returm `true` on success.
     */
    virtual bool apply_snapshot(snapshot& s) = 0;

    /**
     * (Deprecated)
     * Read the given snapshot chunk.
     * This API is for snapshot sender (i.e., leader).
     *
     * @param s Snapshot instance to read.
     * @param offset Byte offset of given chunk.
     * @param[out] data Buffer where the read chunk will be stored.
     * @return Amount of bytes read.
     *         0 if failed.
     */
    virtual int read_snapshot_data(snapshot& s,
                                   const ulong offset,
                                   buffer& data) { return 0; }

    /**
     * Read the given snapshot object.
     * This API is for snapshot sender (i.e., leader).
     *
     * Same as above, this is an optional API for users who want to
     * use logical snapshot.
     *
     * @param s Snapshot instance to read.
     * @param[in,out] user_snp_ctx
     *     User-defined instance that needs to be passed through
     *     the entire snapshot read. It can be a pointer to
     *     state machine specific iterators, or whatever.
     *     On the first `read_logical_snp_obj` call, it will be
     *     set to `null`, and this API may return a new pointer if necessary.
     *     Returned pointer will be passed to next `read_logical_snp_obj`
     *     call.
     * @param obj_id Object ID to read.
     * @param[out] data Buffer where the read object will be stored.
     * @param[out] is_last_obj Set `true` if this is the last object.
     * @return Negative number if failed.
     */
    virtual int read_logical_snp_obj(snapshot& s,
                                     void*& user_snp_ctx,
                                     ulong obj_id,
                                     ptr<buffer>& data_out,
                                     bool& is_last_obj) {
        data_out = buffer::alloc(4); // A dummy buffer.
        is_last_obj = true;
        return 0;
    }

    /**
     * Free user-defined instance that is allocated by
     * `read_logical_snp_obj`.
     * This is an optional API for users who want to use logical snapshot.
     *
     * @param user_snp_ctx User-defined instance to free.
     */
    virtual void free_user_snp_ctx(void*& user_snp_ctx) {}

    /**
     * Get the latest snapshot instance.
     *
     * This API will be invoked at the initialization of Raft server,
     * so that the last last snapshot should be durable for server restart,
     * if you want to avoid unnecessary catch-up.
     *
     * @return Pointer to the latest snapshot.
     */
    virtual ptr<snapshot> last_snapshot() = 0;

    /**
     * Get the last committed Raft log number.
     *
     * This API will be invoked at the initialization of Raft server
     * to identify what the last committed point is, so that the last
     * committed index number should be durable for server restart,
     * if you want to avoid unnecessary catch-up.
     *
     * @return Last committed Raft log number.
     */
    virtual ulong last_commit_index() = 0;

    /**
     * Create a snapshot corresponding to the given info.
     *
     * @param s Snapshot info to create.
     * @param when_done Callback function that will be called after
     *                  snapshot creation is done.
     */
    virtual void create_snapshot(snapshot& s,
                                 async_result<bool>::handler_type& when_done) = 0;

    /**
     * Decide to create snapshot or not.
     * Once the pre-defined condition is satisfied, Raft core will invoke
     * this function to ask if it needs to create a new snapshot.
     * If user-defined state machine does not want to create snapshot
     * at this time, this function will return `false`.
     *
     * @return `true` if wants to create snapshot.
     *         `false` if does not want to create snapshot.
     */
    virtual bool chk_create_snapshot() { return true; }

    /**
     * Decide to transfer leadership.
     * Once the other conditions are met, Raft core will invoke
     * this function to ask if it is allowed to transfer the
     * leadership to other member.
     *
     * @return `true` if wants to transfer leadership.
     *         `false` if not.
     */
    virtual bool allow_leadership_transfer() { return true; }
};

}

#endif //_STATE_MACHINE_HXX_
