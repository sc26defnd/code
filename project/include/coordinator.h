#pragma once

#include "tinyxml2.h"
#include "metadata.h"
#include "proxy.h"
#include <mutex>
#include <condition_variable>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

namespace ECProject
{
  class Coordinator
  {
  public:
    Coordinator(std::string ip, int port, std::string xml_path);
    ~Coordinator();

    void run();
    // rpc, coordinator.cpp
    std::string checkalive(std::string msg);
    // set parameters
    void set_erasure_coding_parameters(ParametersInfo paras);
    // set, return proxy's ip and port
    SetResp request_set(std::vector<std::string> object_keys,
        std::vector<size_t> object_sizes,
        std::vector<unsigned int> object_accessrates);
    void commit_object(std::vector<std::string> keys, bool commit);
    // get, return size of value
    size_t request_get(std::string key, std::string client_ip, int client_port);
    // delete
    void request_delete_by_stripe(std::vector<unsigned int> stripe_ids);
    // repair, repair a list of blocks in specified stripes (stripe_id>=0) or nodes (stripe_id=-1)
    RepairResp request_repair(std::vector<unsigned int> failed_ids, int stripe_id, int maintenance_type = 0);
    // merge
    MergeResp request_merge(int step_size);

    // scale, test
    ScaleResp request_scale(float storage_overhead_upper, float gamma);

    // Redundancy transition, now support Azure-LRC, Uniform-Cauchy-LRC, PC
    TransResp request_redundancy_transition(bool optimized);

    // others
    std::vector<unsigned int> list_stripes();
    // aux.cpp
    void init_ec_schema(std::string config_file);

  private:
    // aux.cpp
    void init_cluster_info();
    void init_proxy_info();
    void reset_metadata();
    Stripe& new_stripe(size_t block_size, ErasureCode *ec);
    ErasureCode* new_ec_for_merge(int step_size);
    void find_out_stripe_partitions(unsigned int stripe_id);
    void init_placement_info(PlacementInfo &placement, std::string key,
                             size_t value_len, size_t block_size, bool isreplica);
    bool if_subject_to_fault_tolerance_lrc(
            ErasureCode *ec, std::vector<int> blocks_in_cluster,
            std::unordered_map<int, std::vector<int>> &group_blocks);
    bool if_subject_to_fault_tolerance_pc(
            ErasureCode *ec, std::vector<int> blocks_in_cluster,
            std::unordered_map<int, std::vector<int>> &col_blocks);
    void write_logs(Logger::LogLevel level, std::string& msg);

    // placement.cpp
    // placement: partition -> place, a partition in a seperate region(cluster)
    void generate_placement_ec(unsigned int stripe_id);
    void generate_placement_replica(unsigned int stripe_id);
    // node selection
    void select_nodes_by_random(std::vector<unsigned int>& free_clusters,
                                std::vector<unsigned int>& block2nodes,
                                unsigned int stripe_id, int split_idx);
    void select_nodes_in_order(std::vector<unsigned int>& block2nodes, unsigned int stripe_id);

    void select_nodes_zone_aware(std::vector<unsigned int>& blocks2nodes, unsigned int stripe_id);

    void select_nodes_by_random_for_replica(std::vector<unsigned int>& free_clusters,
                                            std::vector<unsigned int>& block2nodes,
                                            unsigned int stripe_id);
    void print_placement_result(std::string msg);
    void print_placement_result_replica(std::string msg, unsigned int stripe_id);

    // repair.cpp
    void check_out_failures(
            int stripe_id, std::vector<unsigned int> failed_ids,
            std::unordered_map<unsigned int, std::vector<int>>& failure_map);
    bool concrete_repair_plans(int stripe_id,
                               std::vector<unsigned int>& new_blocks2nodes,
                               std::vector<RepairPlan>& repair_plans,
                               std::vector<MainRepairPlan>& main_repairs,
                               std::vector<std::vector<HelpRepairPlan>>& help_repairs);
    bool concrete_repair_plans_new(int stripe_id,
                               std::vector<unsigned int>& new_blocks2nodes,
                               std::vector<RepairPlan>& repair_plans,
                               std::vector<MainRepairPlan>& main_repairs,
                               std::vector<std::vector<HelpRepairPlan>>& help_repairs);                           
    bool concrete_repair_plans_pc(int stripe_id,
                                  std::vector<unsigned int>& new_blocks2nodes,
                                  std::vector<RepairPlan>& repair_plans,
                                  std::vector<MainRepairPlan>& main_repairs,
                                  std::vector<std::vector<HelpRepairPlan>>& help_repairs);
    void do_repair(std::vector<unsigned int> failed_ids, int stripe_id,
                   RepairResp& response, int maintenance_type = 0);
    void simulation_repair(std::vector<MainRepairPlan>& main_repair,
                           int& cross_cluster_transfers, int& io_cnt);

    // merge.cpp
    void do_stripe_merging(MergeResp& response, int step_size);
    void rs_merge(MergeResp& response, int step_size);
    void azu_lrc_merge(MergeResp& response, int step_size);
    // void lrc_merge(MergeResp& response, int step_size);
    void pc_merge(MergeResp& response, int step_size);
    void simulation_recalculation(MainRecalPlan& main_plan,
            int& cross_cluster_transfers, int& io_cnt);
    
    // transition.cpp
    void do_redundancy_transition(TransResp& response, bool optimized);
    void new_data_placement_for_flat_to_flat_lrc(unsigned int stripe_id,
            std::vector<unsigned int>& placement_info, int& parity_cnt);
    void new_data_placement_for_optimal_to_optimal_lrc(unsigned int stripe_id,
            std::vector<unsigned int>& placement_info, int& parity_cnt, bool optimized);
    void new_data_placement_for_flat_to_optimal(unsigned int stripe_id,
            const std::vector<unsigned int>& old_placement_info,
            std::vector<unsigned int>& new_placement_info,
            int& data_cnt, int& parity_cnt);
    void new_data_placement_for_flat_to_flat_pc(unsigned int stripe_id,
            std::vector<unsigned int>& placement_info, int& parity_cnt);
    void new_data_placement_for_optimal_to_optimal_pc(unsigned int stripe_id,
            std::vector<unsigned int>& placement_info, int& parity_cnt, bool optimized);
    void new_data_placement_for_flat_to_flat(unsigned int stripe_id,
            std::vector<unsigned int>& placement_info, int& parity_cnt);
    void new_data_placement_for_optimal_to_optimal(unsigned int stripe_id,
            std::vector<unsigned int>& placement_info, int& parity_cnt, bool optimized);
    void generate_encoding_plan_lrc(unsigned int stripe_id,
            std::vector<EncodePlan>& plans,
            const std::vector<unsigned int>& new_placement_info, bool optimized);
    void generate_encoding_plan_pc(unsigned int stripe_id,
            std::vector<EncodePlan>& plans,
            const std::vector<unsigned int>& new_placement_info, bool optimized);

    std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_{nullptr};
    std::unordered_map<std::string, std::unique_ptr<coro_rpc::coro_rpc_client>> proxies_;
    ECSchema ec_schema_;
    std::unordered_map<unsigned int, Cluster> cluster_table_;
    std::unordered_map<unsigned int, Node> node_table_;
    std::unordered_map<unsigned int, Stripe> stripe_table_;
    std::unordered_map<std::string, ObjectInfo> commited_object_table_;
    std::unordered_map<std::string, ObjectInfo> updating_object_table_;

    std::mutex mutex_;
    std::condition_variable cv_;
    unsigned int cur_stripe_id_;
    int num_of_clusters_;
    int num_of_nodes_per_cluster_;
    std::string ip_;
    int port_;
    std::string xml_path_;
    double time_;
    unsigned int cur_block_id_;
    unsigned int lucky_cid_;
    std::vector<std::vector<unsigned int>> merge_groups_;
    std::vector<unsigned int> free_clusters_;
    bool merged_flag_ = false;
    Logger* logger_;
  };
}
