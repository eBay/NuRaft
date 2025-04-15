#pragma once

#include "raft_package_asio.hxx"

inline int launch_servers(const std::vector<RaftAsioPkg*>& pkgs,
                   bool enable_ssl,
                   bool use_global_asio = false,
                   bool use_bg_snapshot_io = true,
                   const raft_server::init_options & opt = raft_server::init_options())
{
    size_t num_srvs = pkgs.size();
    CHK_GT(num_srvs, 0);

    for (auto& entry: pkgs) {
        RaftAsioPkg* pp = entry;
        pp->initServer(enable_ssl, use_global_asio, use_bg_snapshot_io, opt);
    }
    // Wait longer than upper timeout.
    TestSuite::sleep_sec(1);
    return 0;
}

inline int make_group(const std::vector<RaftAsioPkg*>& pkgs) {
    size_t num_srvs = pkgs.size();
    CHK_GT(num_srvs, 0);

    RaftAsioPkg* leader = pkgs[0];

    for (size_t ii = 1; ii < num_srvs; ++ii) {
        RaftAsioPkg* ff = pkgs[ii];

        // Add to leader.
        leader->raftServer->add_srv( *(ff->getTestMgr()->get_srv_config()) );

        // Wait longer than upper timeout.
        TestSuite::sleep_sec(1);
    }
    return 0;
}

inline void async_handler(std::list<ulong>* idx_list,
                          std::mutex* idx_list_lock,
                          ptr<buffer>& result,
                          ptr<std::exception>& err)
{
    result->pos(0);
    ulong idx = result->get_ulong();
    if (idx_list) {
        std::lock_guard<std::mutex> l(*idx_list_lock);
        idx_list->push_back(idx);
    }
}

