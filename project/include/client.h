#pragma once

#include "coordinator.h"
#include "metadata.h"
#include <ylt/coro_rpc/coro_rpc_client.hpp>

namespace ECProject {
  class Client {
  public:
    Client(std::string ip, int port, std::string coordinator_ip, int coordinator_port);
    ~Client();

    void set_ec_parameters(ParametersInfo parameters);
    // set
    double set(const std::string& value,
               const std::vector<std::string>& object_keys,
               const std::vector<size_t>& object_sizes,
               const std::vector<unsigned int>& object_accessrates);
    // get
    std::string get(std::string key);
    // delete
    void delete_stripe(unsigned int stripe_id);
    void delete_all_stripes();
    // repair
    RepairResp nodes_repair(std::vector<unsigned int> failed_node_ids);
    RepairResp blocks_repair(std::vector<unsigned int> failed_block_ids, int stripe_id, int maintenance_type = 0);
    // merge
    MergeResp merge(int step_size);
    // scale
    ScaleResp scale(float storage_overhead_upper, float gamma);
    // redundancy transition
    TransResp redundancy_transition(bool optimized);
    // others
    std::vector<unsigned int> list_stripes();

  private:
    std::unique_ptr<coro_rpc::coro_rpc_client> rpc_coordinator_{nullptr};
    int port_;
    std::string ip_;
    std::string coordinator_ip_;
    int coordinator_port_;
    asio::io_context io_context_{};
    asio::ip::tcp::acceptor acceptor_;
  };
};