#include "coordinator.h"

namespace ECProject
{
  void Coordinator::do_repair(
          std::vector<unsigned int> failed_ids, int stripe_id,
          RepairResp& response, int maintenance_type)
  {
    struct timeval start_time, end_time;
    struct timeval m_start_time, m_end_time;
    double repair_time = 0;
    double decoding_time = 0;
    double cross_cluster_time = 0;
    double meta_time = 0;
    int cross_cluster_transfers = 0;
    int io_cnt = 0;
    std::unordered_map<unsigned int, std::vector<int>> failures_map;
    check_out_failures(stripe_id, failed_ids, failures_map);
    for (auto& pair : failures_map) {
      gettimeofday(&start_time, NULL);
      gettimeofday(&m_start_time, NULL);
      Stripe& stripe = stripe_table_[pair.first];

      
      std::vector<int> logical_unavailable_blocks = pair.second; 
      
      if (maintenance_type == 3 || maintenance_type == 4) {
          int target_block_idx = pair.second[0];
          unsigned int target_node_id = stripe.blocks2nodes[target_block_idx];
          unsigned int target_rack_id = node_table_[target_node_id].map2cluster;
          unsigned int target_zone_id = node_table_[target_node_id].zone_id; 

          int n = stripe.ec->k + stripe.ec->m;
          for (int j = 0; j < n; j++) {
              if (j == target_block_idx) continue;
              
              unsigned int n_id = stripe.blocks2nodes[j];
              
              if (maintenance_type == 3 && node_table_[n_id].map2cluster == target_rack_id) {
                  logical_unavailable_blocks.push_back(j); // rack
              } else if (maintenance_type == 4 && node_table_[n_id].zone_id == target_zone_id) {
                  logical_unavailable_blocks.push_back(j); // zone
              }
          }
      }


      stripe.ec->placement_rule = ec_schema_.placement_rule;
      stripe.ec->generate_partition();
      find_out_stripe_partitions(pair.first);
      if (IF_DEBUG) {
        std::string msg = "";
        msg += "Stripe " + std::to_string(pair.first) + " block placement:\n";
        for (auto& vec : stripe.ec->partition_plan) {
          unsigned int node_id = stripe.blocks2nodes[vec[0]];
          unsigned int cluster_id = node_table_[node_id].map2cluster;
          msg += std::to_string(cluster_id) + ": ";
          for (int ele : vec) {
            unsigned int curr_node_id = stripe.blocks2nodes[ele];

            msg += "B" + std::to_string(ele) + 
                   "N" + std::to_string(curr_node_id);
            if (ec_schema_.if_zone_aware) {
              unsigned int zone_id = node_table_[curr_node_id].zone_id;
              msg += "Z" + std::to_string(zone_id);
            }
            msg += " ";
          }
          msg += "\n";
        }
        msg += "Generating repair plan for failures { ";
        for (auto& failure : pair.second) {
          msg += std::to_string(failure) + " ";
        }
        msg += "}\n";
        write_logs(Logger::LogLevel::DEBUG, msg);
      }

      std::vector<RepairPlan> all_repair_plans;
      bool flag = stripe.ec->generate_repair_plan(logical_unavailable_blocks, all_repair_plans,
                                                  ec_schema_.partial_scheme,
                                                  ec_schema_.repair_priority,
                                                  ec_schema_.repair_method);
      if (!flag) {
        response.success = false;
        return;
      }

      std::vector<RepairPlan> repair_plans;
      int original_target_idx = pair.second[0]; 
      
      std::unordered_set<int> required_blocks;
      required_blocks.insert(original_target_idx);

      std::vector<bool> plan_included(all_repair_plans.size(), false);
      bool changed = true;

      
      while (changed) {
          changed = false;
          for (size_t i = 0; i < all_repair_plans.size(); i++) {
              if (plan_included[i]) continue;

              bool is_needed = false;
              for (int f_idx : all_repair_plans[i].failure_idxs) {
                  if (required_blocks.find(f_idx) != required_blocks.end()) {
                      is_needed = true;
                      break;
                  }
              }

              if (is_needed) {
                  plan_included[i] = true;
                  changed = true;

                  
                  for (int f_idx : all_repair_plans[i].failure_idxs) {
                      required_blocks.insert(f_idx);
                  }

                  for (auto& help_group : all_repair_plans[i].help_blocks) {
                      for (int h_idx : help_group) {
                          if (std::find(logical_unavailable_blocks.begin(), logical_unavailable_blocks.end(), h_idx) != logical_unavailable_blocks.end()) {
                              required_blocks.insert(h_idx); 
                          }
                      }
                  }
              }
          }
      }

      
      for (size_t i = 0; i < all_repair_plans.size(); i++) {
          if (plan_included[i]) {
              RepairPlan plan_to_keep = all_repair_plans[i];              
              repair_plans.push_back(plan_to_keep);
          }
      }

//      std::vector<RepairPlan> repair_plans;
//      bool flag = stripe.ec->generate_repair_plan(pair.second, repair_plans,
//                                                  ec_schema_.partial_scheme,
//                                                  ec_schema_.repair_priority,
//                                                  ec_schema_.repair_method);
//      if (!flag) {
//        response.success = false;
//        return;
//      }

      if (IF_DEBUG) {
        std::string msg = "Repair Plan:\n";
        for (int i = 0; i < int(repair_plans.size()); i++) {
          RepairPlan& tmp = repair_plans[i];
          msg += "> Failed Blocks: ";
          for (int j = 0; j < int(tmp.failure_idxs.size()); j++) {
            msg += std::to_string(tmp.failure_idxs[j]) + " ";
          }
          msg += "\n> Repair by Blocks: ";
          for (auto& help_blocks : tmp.help_blocks) {
            for(auto& block : help_blocks) {
              msg += std::to_string(block) + " ";
            }
          }
          msg += "\n> local_or_column: " + std::to_string(tmp.local_or_column)
                 + "\nParity idx: ";
          for (auto& idx : tmp.parity_idxs) {
            msg += std::to_string(idx) + " ";
          }
          msg += "\n";
          write_logs(Logger::LogLevel::DEBUG, msg);
        }
      }
      std::vector<MainRepairPlan> main_repairs;
      std::vector<std::vector<HelpRepairPlan>> help_repairs;
      std::vector<unsigned int> new_blocks2nodes;
      if (check_ec_family(ec_schema_.ec_type) == PCs) {
        concrete_repair_plans_pc(pair.first, new_blocks2nodes, repair_plans, main_repairs, help_repairs);
      }
      else {
        concrete_repair_plans(pair.first, new_blocks2nodes, repair_plans, main_repairs, help_repairs);
      }
      
      if (IF_DEBUG) {
        std::string msg = "Finish generate repair plan.\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      auto lock_ptr = std::make_shared<std::mutex>();

      auto send_main_repair_plan = 
          [this, main_repairs, lock_ptr, &decoding_time, &cross_cluster_time](
              int i, int main_cluster_id) mutable
      {
        std::string chosen_proxy = cluster_table_[main_cluster_id].proxy_ip +
            std::to_string(cluster_table_[main_cluster_id].proxy_port);
        auto resp = 
            async_simple::coro::syncAwait(proxies_[chosen_proxy]
                ->call_for<&Proxy::main_repair>(
                    std::chrono::seconds{600}, main_repairs[i])).value();
        lock_ptr->lock();
        decoding_time += resp.decoding_time;
        cross_cluster_time += resp.cross_cluster_time;
        lock_ptr->unlock();
        if (IF_DEBUG) {
          std::string msg = "Selected main proxy " + chosen_proxy + " of cluster"
                        + std::to_string(main_cluster_id) + ". Decoding time : "
                        + std::to_string(decoding_time) + "\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }
      };

      auto logger_lock_ptr = std::make_shared<std::mutex>();
      auto send_help_repair_plan = 
          [this, help_repairs, logger_lock_ptr](
              int i, int j, std::string proxy_ip, int proxy_port) mutable
      {
        std::string chosen_proxy = proxy_ip + std::to_string(proxy_port);
        async_simple::coro::syncAwait(proxies_[chosen_proxy]
            ->call_for<&Proxy::help_repair>(std::chrono::seconds{600}, help_repairs[i][j]));
        if (IF_DEBUG) {
          logger_lock_ptr->lock();
          std::string msg = "[Thread" + std::to_string(j) + "] Selected help proxy "
                            + chosen_proxy + ".\n";
          write_logs(Logger::LogLevel::INFO, msg);
          logger_lock_ptr->unlock();
        }
      };

      // simulation
      simulation_repair(main_repairs, cross_cluster_transfers, io_cnt);
      if (IF_DEBUG) {
        std::string msg = "Finish sending repair plans. cross-cluster-transfer = "
                          + std::to_string(cross_cluster_transfers) + "\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }
      gettimeofday(&m_end_time, NULL);
      meta_time += m_end_time.tv_sec - m_start_time.tv_sec +
          (m_end_time.tv_usec - m_start_time.tv_usec) * 1.0 / 1000000;

      if (!IF_SIMULATION) {
        for (int i = 0; i < int(main_repairs.size()); i++) {
          try
          {
            MainRepairPlan& tmp = main_repairs[i];
            int failed_num = int(tmp.failed_blocks_index.size());
            unsigned int main_cluster_id = tmp.cluster_id;
            std::thread my_main_thread(send_main_repair_plan, i, main_cluster_id);
            std::vector<std::thread> senders;
            int index = 0;
            for (int j = 0; j < int(tmp.help_clusters_blocks_info.size()); j++) {
              int num_of_blocks_in_help_cluster = 
                  (int)tmp.help_clusters_blocks_info[j].size();
              my_assert(num_of_blocks_in_help_cluster == 
                  int(help_repairs[i][j].inner_cluster_help_blocks_info.size()));
              bool t_flag = tmp.help_clusters_partial_less[j];
              if ((IF_DIRECT_FROM_NODE && ec_schema_.partial_decoding
                   && t_flag) ||
                  !IF_DIRECT_FROM_NODE)
              {
                Cluster &cluster = cluster_table_[help_repairs[i][j].cluster_id];
                senders.push_back(std::thread(send_help_repair_plan, i, j,
                                  cluster.proxy_ip, cluster.proxy_port));
              }
            }
            for (int j = 0; j < int(senders.size()); j++) {
              senders[j].join();
            }
            my_main_thread.join();
          }
          catch(const std::exception& e)
          {
            std::cerr << e.what() << '\n';
          }
        }
      }
      // update metadata
      stripe.blocks2nodes.clear();
      stripe.blocks2nodes.insert(stripe.blocks2nodes.end(), new_blocks2nodes.begin(),
                                 new_blocks2nodes.end());
      gettimeofday(&end_time, NULL);
      double temp_time = end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      repair_time += temp_time;

      std::string msg = "Repair { ";
      for (auto& failure : pair.second) {
        msg += std::to_string(failure) + " ";
      }
      msg += "} total = " + std::to_string(repair_time) + "s, latest = "
                + std::to_string(temp_time) + "s. Decode: total = "
                + std::to_string(decoding_time) + "\n";
      write_logs(Logger::LogLevel::INFO, msg);
    }
    response.decoding_time = decoding_time;
    response.cross_cluster_time = cross_cluster_time;
    response.repair_time = repair_time;
    response.meta_time = meta_time;
    response.cross_cluster_transfers = cross_cluster_transfers;
    response.io_cnt = io_cnt;
    response.success = true;
  }

  void Coordinator::check_out_failures(
          int stripe_id, std::vector<unsigned int> failed_ids,
          std::unordered_map<unsigned int, std::vector<int>> &failures_map)
  {
    if (stripe_id >= 0) {   // block failures
      for (auto id : failed_ids) {
        failures_map[stripe_id].push_back((int)id);
      }
    } else {   // node failures
      int num_of_failed_nodes = int(failed_ids.size());
      for (int i = 0; i < num_of_failed_nodes; i++) {
        unsigned int node_id = failed_ids[i];
        for (auto it = stripe_table_.begin(); it != stripe_table_.end(); it++) {
          int t_stripe_id = it->first;
          auto& stripe = it->second;
          int n = stripe.ec->k + stripe.ec->m;
          int failed_block_idx = -1;
          for (int j = 0; j < n; j++) {
            if (stripe.blocks2nodes[j] == node_id) {
              failed_block_idx = j;
              break;
            }
          }
          if (failures_map.find(t_stripe_id) != failures_map.end()) {
            failures_map[t_stripe_id].push_back(failed_block_idx);
          } else {
            std::vector<int> failed_blocks;
            failed_blocks.push_back(failed_block_idx);
            failures_map[t_stripe_id] = failed_blocks;
          }
        }
      }
    }
  }

  bool Coordinator::concrete_repair_plans(
          int stripe_id,
          std::vector<unsigned int>& new_blocks2nodes,
          std::vector<RepairPlan>& repair_plans,
          std::vector<MainRepairPlan>& main_repairs,
          std::vector<std::vector<HelpRepairPlan>>& help_repairs)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    new_blocks2nodes.clear();
    new_blocks2nodes.insert(new_blocks2nodes.end(), stripe.blocks2nodes.begin(),
        stripe.blocks2nodes.end());
    // for new locations, to optimize
    std::unordered_map<unsigned int, std::vector<unsigned int>> free_nodes_in_clusters;
    for (auto& repair_plan : repair_plans) {
      std::unordered_map<int, unsigned int> map2clusters;
      int cnt = 0;
      for (auto& help_block : repair_plan.help_blocks) {
        unsigned int nid = new_blocks2nodes[help_block[0]];
        unsigned int cid = node_table_[nid].map2cluster;
        map2clusters[cnt++] = cid;
      }

      // failed largest first
      std::unordered_map<unsigned int, int> failures_cnt;
      unsigned int main_cid = 0;
      int max_cnt_val = 0;
      for (auto& idx : repair_plan.failure_idxs) {
        unsigned int nid = new_blocks2nodes[idx];
        unsigned int cid = node_table_[nid].map2cluster;
        if (failures_cnt.find(cid) == failures_cnt.end()) {
          failures_cnt[cid] = 1;
        } else {
          failures_cnt[cid]++;
        }
        if (failures_cnt[cid] > max_cnt_val) {
          max_cnt_val = failures_cnt[cid];
          main_cid = cid;
        }
      }

//////////////////////////////////////////////////////////////
//      unsigned int target_idx = repair_plan.failure_idxs[0]; 
//      unsigned int target_nid = new_blocks2nodes[target_idx];

//      unsigned int main_cid = node_table_[target_nid].map2cluster;  
//////////////////////////////////////////////////////////////
      std::unordered_set<unsigned int> failed_cluster_sets;
      for (auto it = repair_plan.failure_idxs.begin();
           it != repair_plan.failure_idxs.end(); it++) {
        unsigned int node_id = new_blocks2nodes[*it];
        unsigned int cluster_id = node_table_[node_id].map2cluster;
        failed_cluster_sets.insert(cluster_id);
        if (free_nodes_in_clusters.find(cluster_id) ==
            free_nodes_in_clusters.end()) {
          std::vector<unsigned int> free_nodes;
          Cluster &cluster = cluster_table_[cluster_id];
          for (int i = 0; i < num_of_nodes_per_cluster_; i++) {
            free_nodes.push_back(cluster.nodes[i]);
          }
          free_nodes_in_clusters[cluster_id] = free_nodes;
        }
        auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                              free_nodes_in_clusters[cluster_id].end(), node_id);
        if (iter != free_nodes_in_clusters[cluster_id].end()) {
          //free_nodes_in_clusters[cluster_id].erase(iter);
        }
      }
      MainRepairPlan main_plan;
      int clusters_num = repair_plan.help_blocks.size();
      CodingParameters cp;
      ec_schema_.ec->get_coding_parameters(cp);
      for (int i = 0; i < clusters_num; i++) {
        for (auto block_idx : repair_plan.help_blocks[i]) {
          main_plan.live_blocks_index.push_back(block_idx);
        }
        if (failed_cluster_sets.find(map2clusters[i]) != failed_cluster_sets.end()) {
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = new_blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            main_plan.inner_cluster_help_blocks_info.push_back(
                std::make_pair(block_idx, std::make_pair(node_ip, node_port)));
            main_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
            // for new locations
            unsigned int cluster_id = node_table_[node_id].map2cluster;
            auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                                  free_nodes_in_clusters[cluster_id].end(), node_id);
            if (iter != free_nodes_in_clusters[cluster_id].end()) {
              free_nodes_in_clusters[cluster_id].erase(iter);
            }
          }
        }
      }
      main_plan.ec_type = ec_schema_.ec_type;
      stripe.ec->get_coding_parameters(main_plan.cp);
      main_plan.cluster_id = main_cid;
      main_plan.cp.x = ec_schema_.x;
      main_plan.cp.seri_num = stripe_id % ec_schema_.x;
      main_plan.cp.local_or_column = repair_plan.local_or_column;
      main_plan.block_size = stripe.block_size;
      main_plan.partial_decoding = ec_schema_.partial_decoding;
      main_plan.partial_scheme = ec_schema_.partial_scheme;
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        main_plan.failed_blocks_index.push_back(*it);
        main_plan.failed_block_ids.push_back(stripe.block_ids[*it]);
      }
      for (auto block_idx : repair_plan.parity_idxs) {
        main_plan.parity_blocks_index.push_back(block_idx);
      }
      std::vector<HelpRepairPlan> help_plans;
      for (int i = 0; i < clusters_num; i++) {
        if (map2clusters[i] != main_cid) {
          HelpRepairPlan help_plan;
          help_plan.ec_type = main_plan.ec_type;
          stripe.ec->get_coding_parameters(help_plan.cp);
          help_plan.cluster_id = map2clusters[i];
          help_plan.block_size = main_plan.block_size;
          help_plan.partial_decoding = main_plan.partial_decoding;
          help_plan.partial_scheme = main_plan.partial_scheme;
          help_plan.isvertical = main_plan.isvertical;
          if (ec_schema_.partial_scheme) {
            for(auto it = repair_plan.failure_idxs.begin();
              it != repair_plan.failure_idxs.end(); it++) {
              help_plan.failed_blocks_index.push_back(*it);
            }
          }
          for(auto it = main_plan.parity_blocks_index.begin(); 
              it != main_plan.parity_blocks_index.end(); it++) {
            help_plan.parity_blocks_index.push_back(*it);
          }
          for(auto it = main_plan.live_blocks_index.begin(); 
              it != main_plan.live_blocks_index.end(); it++) {
            help_plan.live_blocks_index.push_back(*it);
          }
          int num_of_help_blocks = 0;
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = new_blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            help_plan.inner_cluster_help_blocks_info.push_back(
                std::make_pair(block_idx, std::make_pair(node_ip, node_port)));
            help_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
            num_of_help_blocks++;
          }
          if (ec_schema_.partial_scheme) {
            int failed_num = (int) help_plan.failed_blocks_index.size();
            if (num_of_help_blocks > failed_num) {
              help_plan.partial_less = true;
            }
          } else {
            int num_of_partial_blocks =
                ec_schema_.ec->num_of_partial_blocks_to_transfer(
                    repair_plan.help_blocks[i], help_plan.parity_blocks_index);
            if (num_of_partial_blocks < num_of_help_blocks) {
              help_plan.partial_less = true;
            }
          }
          main_plan.help_clusters_partial_less.push_back(help_plan.partial_less);
          main_plan.help_clusters_blocks_info.push_back(
              help_plan.inner_cluster_help_blocks_info);
          main_plan.help_clusters_block_ids.push_back(
              help_plan.inner_cluster_help_block_ids);
          help_plan.main_proxy_ip = cluster_table_[main_cid].proxy_ip;
          help_plan.main_proxy_port =
              cluster_table_[main_cid].proxy_port + SOCKET_PORT_OFFSET;
          help_plans.push_back(help_plan);
        }
      }
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        unsigned int old_node_id = new_blocks2nodes[*it];
        unsigned int cluster_id = node_table_[old_node_id].map2cluster;
        unsigned int target_zone = node_table_[old_node_id].zone_id;
        std::vector<unsigned int> &free_nodes = free_nodes_in_clusters[cluster_id];
        unsigned int new_node_id = 0;

        std::vector<unsigned int> candidates;
        if (ec_schema_.if_zone_aware) {
            for (unsigned int nid : free_nodes) {
                if (node_table_[nid].zone_id == target_zone) {
                    candidates.push_back(nid);
                }
            }
        }
        if (!candidates.empty()) {
            int ran_idx = random_index(candidates.size());
            new_node_id = candidates[ran_idx];
        } else {
            int ran_idx = random_index(free_nodes.size());
            new_node_id = free_nodes[ran_idx];
        }
       // unsigned int node_id = new_blocks2nodes[*it];
       // unsigned int cluster_id = node_table_[node_id].map2cluster;
       // std::vector<unsigned int> &free_nodes = free_nodes_in_clusters[cluster_id];
       //int ran_node_idx = -1;
       // unsigned int new_node_id = 0;
       // ran_node_idx = random_index(free_nodes.size());
       // new_node_id = free_nodes[ran_node_idx];
        auto iter = std::find(free_nodes.begin(), free_nodes.end(), new_node_id);
        if (iter != free_nodes.end()) {
          free_nodes.erase(iter);
        }
        new_blocks2nodes[*it] = new_node_id;
        std::string node_ip = node_table_[new_node_id].node_ip;
        int node_port = node_table_[new_node_id].node_port;
        main_plan.new_locations.push_back(
            std::make_pair(cluster_id, std::make_pair(node_ip, node_port)));
      }
      main_repairs.push_back(main_plan);
      help_repairs.push_back(help_plans);
    }
    return true;
  }


  bool Coordinator::concrete_repair_plans_pc(
          int stripe_id,
          std::vector<unsigned int>& new_blocks2nodes,
          std::vector<RepairPlan>& repair_plans,
          std::vector<MainRepairPlan>& main_repairs,
          std::vector<std::vector<HelpRepairPlan>>& help_repairs)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    new_blocks2nodes.clear();
    new_blocks2nodes.insert(new_blocks2nodes.end(), stripe.blocks2nodes.begin(),
        stripe.blocks2nodes.end());
    std::unordered_map<unsigned int, std::vector<unsigned int>> free_nodes_in_clusters;
    CodingParameters cp;
    stripe.ec->get_coding_parameters(cp);
    ProductCode pc;
    pc.init_coding_parameters(cp);
    for (auto& repair_plan : repair_plans) {
      std::unordered_map<int, unsigned int> map2clusters;
      int cnt = 0;
      for (auto& help_block : repair_plan.help_blocks) {
        unsigned int nid = new_blocks2nodes[help_block[0]];
        unsigned int cid = node_table_[nid].map2cluster;
        map2clusters[cnt++] = cid;
      }
      std::unordered_map<unsigned int, int> failures_cnt;
      unsigned int main_cid = 0;
      int max_cnt_val = 0;
      for (auto& idx : repair_plan.failure_idxs) {
        unsigned int nid = new_blocks2nodes[idx];
        unsigned int cid = node_table_[nid].map2cluster;
        if (failures_cnt.find(cid) == failures_cnt.end()) {
          failures_cnt[cid] = 1;
        } else {
          failures_cnt[cid]++;
        }
        if (failures_cnt[cid] > max_cnt_val) {
          max_cnt_val = failures_cnt[cid];
          main_cid = cid;
        }
      }
      // for new locations, to optimize
      std::unordered_set<unsigned int> failed_cluster_sets;
      for (auto it = repair_plan.failure_idxs.begin();
           it != repair_plan.failure_idxs.end(); it++) {
        unsigned int node_id = new_blocks2nodes[*it];
        unsigned int cluster_id = node_table_[node_id].map2cluster;
        failed_cluster_sets.insert(cluster_id);
        if (free_nodes_in_clusters.find(cluster_id) ==
            free_nodes_in_clusters.end()) {
          std::vector<unsigned int> free_nodes;
          Cluster &cluster = cluster_table_[cluster_id];
          for (int i = 0; i < num_of_nodes_per_cluster_; i++) {
            free_nodes.push_back(cluster.nodes[i]);
          }
          free_nodes_in_clusters[cluster_id] = free_nodes;
        }
        auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                              free_nodes_in_clusters[cluster_id].end(), node_id);
        if (iter != free_nodes_in_clusters[cluster_id].end()) {
          free_nodes_in_clusters[cluster_id].erase(iter);
        }
      }
      int row = -1, col = -1;
      MainRepairPlan main_plan;
      if (ec_schema_.multistripe_placement_rule == VERTICAL) {
          main_plan.isvertical = true;
      }
      int clusters_num = repair_plan.help_blocks.size();
      for (int i = 0; i < clusters_num; i++) {
        for (auto block_idx : repair_plan.help_blocks[i]) {
          pc.bid2rowcol(block_idx, row, col);
          if (repair_plan.local_or_column) {
            main_plan.live_blocks_index.push_back(row);
          } else {
            main_plan.live_blocks_index.push_back(col);
          }
        }
        if (failed_cluster_sets.find(map2clusters[i]) != failed_cluster_sets.end()) {
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = new_blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            pc.bid2rowcol(block_idx, row, col);
            if (repair_plan.local_or_column) {
              main_plan.inner_cluster_help_blocks_info.push_back(
                  std::make_pair(row, std::make_pair(node_ip, node_port)));
            } else {
              main_plan.inner_cluster_help_blocks_info.push_back(
                  std::make_pair(col, std::make_pair(node_ip, node_port)));
            }

            main_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
            
            // for new locations
            unsigned int cluster_id = node_table_[node_id].map2cluster;
            auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                                  free_nodes_in_clusters[cluster_id].end(), node_id);
            if (iter != free_nodes_in_clusters[cluster_id].end()) {
              free_nodes_in_clusters[cluster_id].erase(iter);
            }
          }
        }
      }
      
      main_plan.ec_type = RS;
      if (repair_plan.local_or_column) {
        main_plan.cp.k = pc.k2;
        main_plan.cp.m = pc.m2;
      } else {
        main_plan.cp.k = pc.k1;
        main_plan.cp.m = pc.m1;
      }
      main_plan.cluster_id = main_cid;
      main_plan.cp.x = ec_schema_.x;
      main_plan.cp.seri_num = stripe_id % ec_schema_.x;
      main_plan.cp.local_or_column = repair_plan.local_or_column;
      main_plan.block_size = stripe.block_size;
      main_plan.partial_decoding = ec_schema_.partial_decoding;
      main_plan.partial_scheme = ec_schema_.partial_scheme;
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        pc.bid2rowcol(*it, row, col);
        if (repair_plan.local_or_column) {
          main_plan.failed_blocks_index.push_back(row);
        } else {
          main_plan.failed_blocks_index.push_back(col);
        }
        main_plan.failed_block_ids.push_back(stripe.block_ids[*it]);
      }
      for (auto block_idx : repair_plan.parity_idxs) {
        pc.bid2rowcol(block_idx, row, col);
        if (repair_plan.local_or_column) {
          main_plan.parity_blocks_index.push_back(row);
        } else {
          main_plan.parity_blocks_index.push_back(col);
        }
      }
      std::vector<HelpRepairPlan> help_plans;
      for (int i = 0; i < clusters_num; i++) {
        if (map2clusters[i] != main_cid) {
          HelpRepairPlan help_plan;
          help_plan.ec_type = main_plan.ec_type;
          help_plan.cp = main_plan.cp;
          help_plan.cluster_id = map2clusters[i];
          help_plan.block_size = main_plan.block_size;
          help_plan.partial_decoding = main_plan.partial_decoding;
          help_plan.partial_scheme = main_plan.partial_scheme;
          help_plan.isvertical = main_plan.isvertical;
          if (ec_schema_.partial_scheme) {
            for(auto it = main_plan.failed_blocks_index.begin();
                it != main_plan.failed_blocks_index.end(); it++) {
              help_plan.failed_blocks_index.push_back(*it);
            }
          }
          for(auto it = main_plan.parity_blocks_index.begin(); 
              it != main_plan.parity_blocks_index.end(); it++) {
            help_plan.parity_blocks_index.push_back(*it);
          }
          for(auto it = main_plan.live_blocks_index.begin(); 
              it != main_plan.live_blocks_index.end(); it++) {
            help_plan.live_blocks_index.push_back(*it);
          }
          int num_of_help_blocks = 0;
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = new_blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            pc.bid2rowcol(block_idx, row, col);
            if (repair_plan.local_or_column) {
              help_plan.inner_cluster_help_blocks_info.push_back(
                  std::make_pair(row, std::make_pair(node_ip, node_port)));
            } else {
              help_plan.inner_cluster_help_blocks_info.push_back(
                  std::make_pair(col, std::make_pair(node_ip, node_port)));
            }
            help_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
            num_of_help_blocks++;
          }
          if (ec_schema_.partial_scheme) {
            int failed_num = (int) help_plan.failed_blocks_index.size();
            if (num_of_help_blocks > failed_num) {
              help_plan.partial_less = true;
            }
          } else {
            int num_of_partial_blocks =
                ec_schema_.ec->num_of_partial_blocks_to_transfer(
                    repair_plan.help_blocks[i], help_plan.parity_blocks_index);
            if (num_of_partial_blocks < num_of_help_blocks) {
              help_plan.partial_less = true;
            }
          }
          main_plan.help_clusters_partial_less.push_back(help_plan.partial_less);
          main_plan.help_clusters_blocks_info.push_back(
              help_plan.inner_cluster_help_blocks_info);
          main_plan.help_clusters_block_ids.push_back(
              help_plan.inner_cluster_help_block_ids);
          help_plan.main_proxy_ip = cluster_table_[main_cid].proxy_ip;
          help_plan.main_proxy_port =
              cluster_table_[main_cid].proxy_port + SOCKET_PORT_OFFSET;
          help_plans.push_back(help_plan);
        }
      }
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        unsigned int node_id = new_blocks2nodes[*it];
        unsigned int cluster_id = node_table_[node_id].map2cluster;
        std::vector<unsigned int> &free_nodes = free_nodes_in_clusters[cluster_id];
        int ran_node_idx = -1;
        unsigned int new_node_id = 0;
        ran_node_idx = random_index(free_nodes.size());
        new_node_id = free_nodes[ran_node_idx];
        auto iter = std::find(free_nodes.begin(), free_nodes.end(), new_node_id);
        if (iter != free_nodes.end()) {
          free_nodes.erase(iter);
        }

        new_blocks2nodes[*it] = new_node_id;
        std::string node_ip = node_table_[new_node_id].node_ip;
        int node_port = node_table_[new_node_id].node_port;
        main_plan.new_locations.push_back(
            std::make_pair(cluster_id, std::make_pair(node_ip, node_port)));
      }
      main_repairs.push_back(main_plan);
      help_repairs.push_back(help_plans);
    }
    return true;
  }

  void Coordinator::simulation_repair(
          std::vector<MainRepairPlan>& main_repair,
          int& cross_cluster_transfers,
          int& io_cnt)
  {
    std::string msg = "";
    if (IF_DEBUG) {
      msg += "Simulation:\n"; 
    }
    for (int i = 0; i < int(main_repair.size()); i++) {
      int failed_num = int(main_repair[i].failed_blocks_index.size());
      for (int j = 0; j < int(main_repair[i].help_clusters_blocks_info.size()); j++) {
        int num_of_help_blocks = int(main_repair[i].help_clusters_blocks_info[j].size());
        int num_of_partial_blocks = failed_num;
        if (IF_DEBUG) {
          msg += "Cluster " + std::to_string(j) + ": ";
          for (auto& kv : main_repair[i].help_clusters_blocks_info[j]) {
            msg += std::to_string(kv.first) + " ";
          }
          msg += " | ";
        }
        if (!ec_schema_.partial_scheme) {
        std::vector<int> local_data_idxs;
          for (auto& kv : main_repair[i].help_clusters_blocks_info[j]) {
            local_data_idxs.push_back(kv.first);
          }
          ec_schema_.ec->local_or_column = main_repair[i].cp.local_or_column;
          num_of_partial_blocks =
              ec_schema_.ec->num_of_partial_blocks_to_transfer(
                  local_data_idxs, main_repair[i].parity_blocks_index);
        }
        
        if (num_of_help_blocks > num_of_partial_blocks && ec_schema_.partial_decoding) {
          cross_cluster_transfers += num_of_partial_blocks;
        } else {
          cross_cluster_transfers += num_of_help_blocks;
        }
        io_cnt += num_of_help_blocks;
      }
      for (auto& kv : main_repair[i].new_locations) {
        if (kv.first != main_repair[i].cluster_id) {
          cross_cluster_transfers++;
        }
      }
      io_cnt += failed_num;
      io_cnt += int(main_repair[i].inner_cluster_help_block_ids.size());
    }
    if (IF_DEBUG) {
      msg += "\n";
      write_logs(Logger::LogLevel::DEBUG, msg);
    }
  }
}
