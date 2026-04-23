#include "coordinator.h"

namespace ECProject {
  void Coordinator::do_redundancy_transition(TransResp& response, bool optimized)
  {
    if (ec_schema_.is_ec_now) {
      response.iftransed = false;
      return;
    }
    for (auto& kv : stripe_table_) {
      if (kv.second.cur_block_num < ec_schema_.ec->k) {
        response.iftransed = false;
        return;
      }
    }

    struct timeval start_time, end_time;
    struct timeval m_start_time, m_end_time;
    int tot_stripe_num = int(stripe_table_.size());
    int stripe_cnt = 0;
    double transition_time = 0;
    double encoding_time = 0;
    double cross_cluster_time = 0;
    double meta_time = 0;
    double max_trans_time = 0;
    double min_trans_time = 1000.0;
    int cross_cluster_transfers = 0;
    int io_cnt = 0;
    int data_reloc_cnt = 0;
    int parity_reloc_cnt = 0;
    for (auto& kv : stripe_table_) {
      gettimeofday(&start_time, NULL);
      gettimeofday(&m_start_time, NULL);
      unsigned int stripe_id = kv.first;
      Stripe& stripe = kv.second;

      if (IF_DEBUG) {
        std::string msg = "[Transition] Generate data placement for stripe " + std::to_string(stripe_id) + "!\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }
      // new data placement
      std::vector<unsigned int> new_placement_info;
      new_placement_info.insert(new_placement_info.end(),
          stripe.blocks2nodes.begin(), stripe.blocks2nodes.end());
      if (ec_schema_.placement_rule == FLAT) {
        if (ec_schema_.placement_rule_for_trans == FLAT) {
          new_data_placement_for_flat_to_flat(stripe_id, new_placement_info, parity_reloc_cnt);
        } else if (ec_schema_.placement_rule_for_trans == OPTIMAL) {
          new_data_placement_for_flat_to_optimal(stripe_id, stripe.blocks2nodes,
              new_placement_info, data_reloc_cnt, parity_reloc_cnt);
        } else {
          response.iftransed = false;
          return;
        }
      } else if (ec_schema_.placement_rule == OPTIMAL && ec_schema_.placement_rule_for_trans == OPTIMAL) {
        new_data_placement_for_optimal_to_optimal(stripe_id, new_placement_info, parity_reloc_cnt, optimized);
      } else {
        response.iftransed = false;
        return;
      }

      if (IF_DEBUG) {
        std::string msg = "[Transition] Generate encoding plans!\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }
      // generate encoding plan based on new data placement and replicas2
      std::vector<EncodePlan> encoding_plans;
      if (ec_schema_.ec_type == AZURE_LRC || ec_schema_.ec_type == UNIFORM_CAUCHY_LRC) {
        generate_encoding_plan_lrc(stripe_id, encoding_plans, new_placement_info, optimized);
      } else if (ec_schema_.ec_type == PC) {
        generate_encoding_plan_pc(stripe_id, encoding_plans, new_placement_info, optimized);
      }

      // figure out data block relocations if needed
      std::vector<unsigned int> blocks_to_move;
      std::vector<unsigned int> src_nodes;
      std::vector<unsigned int> des_nodes;
      for (int i = 0; i < stripe.ec->k; i++) {
        int src_node_id = stripe.blocks2nodes[i];
        int des_node_id = new_placement_info[i];
        if (src_node_id != des_node_id) {
          blocks_to_move.push_back(stripe.block_ids[i]);
          src_nodes.push_back(src_node_id);
          des_nodes.push_back(des_node_id);
        }
      }
      RelocatePlan reloc_plan;
      reloc_plan.block_size = ec_schema_.block_size;
      int move_num = (int)blocks_to_move.size();
      if (move_num > 0) {
        for (int i = 0; i < move_num; i++) {
          reloc_plan.blocks_to_move.push_back(blocks_to_move[i]);
          auto& src_node = node_table_[src_nodes[i]];
          reloc_plan.src_nodes.push_back(std::make_pair(src_node.map2cluster,
              std::make_pair(src_node.node_ip, src_node.node_port)));
          auto& des_node = node_table_[des_nodes[i]];
          reloc_plan.des_nodes.push_back(std::make_pair(des_node.map2cluster,
              std::make_pair(des_node.node_ip, des_node.node_port)));
          if (src_node.map2cluster != des_node.map2cluster) {
            cross_cluster_transfers++;
          }
        }
        if (IF_DEBUG) {
          std::string msg = "[Transition] " + std::to_string(move_num) + " blocks to relocate:\n";
          for (int ii = 0; ii < move_num; ii++) {
            msg += std::to_string(blocks_to_move[ii]) + "[" +
                std::to_string(src_nodes[ii]) + "->" +
                std::to_string(des_nodes[ii]) + "] ";
          }
          msg += "\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }
      }

      // delete plan
      DeletePlan replicas_for_delete;
      for (int i = 0; i < stripe.ec->k; i++) {
        Node& t_node = node_table_[stripe.replicas2[i]];
        replicas_for_delete.blocks_info.push_back(
          std::make_pair(t_node.node_ip, t_node.node_port));
        replicas_for_delete.block_ids.push_back(stripe.block_ids[i]);
      }
      if (!ec_schema_.from_two_replica) {
        for (int i = 0; i < stripe.ec->k; i++) {
          Node& t_node = node_table_[stripe.replicas3[i]];
          replicas_for_delete.blocks_info.push_back(
            std::make_pair(t_node.node_ip, t_node.node_port));
          replicas_for_delete.block_ids.push_back(stripe.block_ids[i]);
        }
      }

      gettimeofday(&m_end_time, NULL);
      meta_time += m_end_time.tv_sec - m_start_time.tv_sec +
          (m_end_time.tv_usec - m_start_time.tv_usec) / 1000000;

      // for simulation
      for (auto& plan : encoding_plans) {
        int cnt = 0;
        for (auto& kv : plan.new_locations) {
          if (plan.ec_type == ec_schema_.ec_type) {
            unsigned int node_id = new_placement_info[kv.first];
            unsigned int cluster_id = node_table_[node_id].map2cluster;
            if (cluster_id != plan.cluster_id) {
              ++cnt;
            }
          }
        }
        io_cnt += plan.data_block_ids.size() + plan.new_parity_block_ids.size();
        cross_cluster_transfers += cnt;
        plan.cross_cluster_num = cnt;
      }

      // encoding
      auto lock_ptr = std::make_shared<std::mutex>();
      auto send_encode_plan = [this, encoding_plans,
                            lock_ptr, &encoding_time,
                            &cross_cluster_time](int i) mutable
      {
        unsigned int cid = encoding_plans[i].cluster_id;
        std::string chosen_proxy = cluster_table_[cid].proxy_ip +
            std::to_string(cluster_table_[cid].proxy_port);
        auto resp = async_simple::coro::syncAwait(proxies_[chosen_proxy]
                        ->call_for<&Proxy::encode_stripe>(
                            std::chrono::seconds{600}, encoding_plans[i])).value();
        lock_ptr->lock();
        encoding_time += resp.encoding_time;
        cross_cluster_time += resp.cross_cluster_time;
        lock_ptr->unlock();
        if (IF_DEBUG) {
          std::string msg = "Send encoding plan to proxy " + chosen_proxy + "\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }
      };

      if (!IF_SIMULATION) {
        try {
          if (IF_DEBUG) {
            std::string msg = "[Transition] Send encode plans!\n";
            write_logs(Logger::LogLevel::INFO, msg);
          }
          std::vector<std::thread> senders;
          for (auto i = 0; i < encoding_plans.size(); i++) {
            senders.push_back(std::thread(send_encode_plan, i));
          }
          for (int i = 0; i < int(senders.size()); i++) {
            senders[i].join();
          }
        }
        catch (const std::exception &e)
        {
          std::cerr << e.what() << '\n';
        }
      }

      if (!IF_SIMULATION) {
        // block relocation
        if (move_num > 0) {
          unsigned int ran_cluster_id = (unsigned int)random_index(num_of_clusters_);
          std::string chosen_proxy = cluster_table_[ran_cluster_id].proxy_ip +
              std::to_string(cluster_table_[ran_cluster_id].proxy_port);
          auto resp = async_simple::coro::syncAwait(proxies_[chosen_proxy]
                        ->call_for<&Proxy::block_relocation>(std::chrono::seconds{600}, reloc_plan)).value();
          cross_cluster_time += resp.cross_cluster_time;
          io_cnt += 2 * (int)reloc_plan.blocks_to_move.size();
        }

        // delete replicas
        unsigned int del_cluster_id = (unsigned int)random_index(num_of_clusters_);
        std::string chosen_proxy = cluster_table_[del_cluster_id].proxy_ip +
            std::to_string(cluster_table_[del_cluster_id].proxy_port);
        async_simple::coro::syncAwait(proxies_[chosen_proxy]
            ->call<&Proxy::delete_blocks>(replicas_for_delete));
      }

      // update metadata
      gettimeofday(&m_start_time, NULL);
      stripe.cur_block_num = stripe.ec->k + stripe.ec->m;
      stripe.blocks2nodes.clear();
      stripe.blocks2nodes.insert(stripe.blocks2nodes.end(),
          new_placement_info.begin(), new_placement_info.end());
      stripe.ec->placement_rule = ec_schema_.placement_rule_for_trans;
      for (auto i = stripe.ec->k; i < stripe.ec->k + stripe.ec->m; i++) {
        unsigned int nid = stripe.blocks2nodes[i];
        unsigned int cid = node_table_[nid].map2cluster;
        cluster_table_[cid].holding_stripe_ids.insert(stripe_id);
      }
      gettimeofday(&m_end_time, NULL);
      meta_time += m_end_time.tv_sec - m_start_time.tv_sec +
          (m_end_time.tv_usec - m_start_time.tv_usec) / 1000000;

      gettimeofday(&end_time, NULL);
      double temp_time = end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      transition_time += temp_time;
      max_trans_time = std::max(max_trans_time, temp_time);
      min_trans_time = std::min(min_trans_time, temp_time);

      stripe_cnt++;
      std::string msg = "[Transition] Process " + std::to_string(stripe_cnt) 
                          + "/" + std::to_string(tot_stripe_num) + " total_time:"
                          + std::to_string(transition_time) + " latest:"
                          + std::to_string(temp_time) + "\n";
      write_logs(Logger::LogLevel::INFO, msg);
    }

    if (IF_DEBUG) {
      std::string msg = "[Transition] Finish transition!\n";
      write_logs(Logger::LogLevel::INFO, msg);
      print_placement_result("Latest placement result:");
    }
    
    ec_schema_.is_ec_now = true;
    response.transition_time = transition_time;
    response.encoding_time = encoding_time;
    response.meta_time = meta_time;
    response.cross_cluster_time = cross_cluster_time;
    response.max_trans_time = max_trans_time;
    response.min_trans_time = min_trans_time;
    response.cross_cluster_transfers = cross_cluster_transfers;
    response.io_cnt = io_cnt;
    response.data_reloc_cnt = data_reloc_cnt;
    response.parity_reloc_cnt = parity_reloc_cnt;
    response.iftransed = true;
  }

  void Coordinator::new_data_placement_for_flat_to_flat_lrc(unsigned int stripe_id,
            std::vector<unsigned int>& placement_info, int& parity_cnt)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    auto lrc = dynamic_cast<LocallyRepairableCode*>(stripe.ec);
    placement_info[lrc->k + lrc->g] = stripe.replicas2[0];
    parity_cnt += lrc->g + lrc->l - 1;
  }

  void Coordinator::new_data_placement_for_optimal_to_optimal_lrc(unsigned int stripe_id,
            std::vector<unsigned int>& placement_info, int& parity_cnt, bool optimized)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    auto lrc = dynamic_cast<LocallyRepairableCode*>(stripe.ec);
    int b = lrc->k / lrc->l;
    if (ec_schema_.ec_type == UNIFORM_CAUCHY_LRC) {
      b = ceil(double(lrc->k + lrc->g) / double(lrc->l));
    }
    if (b > lrc->g) {
      if (b % (lrc->g + 1) == 0) {  // global and local
        my_assert(lrc->k >= lrc->g + lrc->l);
        int j = 0;
        for (int i = lrc->k; i < lrc->k + lrc->g + lrc->l; ++i) {
          placement_info[i] = stripe.replicas2[j++];
        }
      } else {  // global
        int j = 0;
        for (int i = lrc->k; i < lrc->k + lrc->g; ++i) {
          placement_info[i] = stripe.replicas2[j++];
        }
        parity_cnt += lrc->l;
        if (ec_schema_.ec_type == UNIFORM_CAUCHY_LRC) {
          placement_info[lrc->k + lrc->g + lrc->l - 1] = stripe.replicas2[j++];
          --parity_cnt;
        }
      }
    } else {
      int j = 0;
      if (ec_schema_.ec_type == UNIFORM_CAUCHY_LRC) {
        int global_group_num = lrc->g / b;
        for (int i = lrc->k + lrc->g - global_group_num * b; i < lrc->k + lrc->g; ++i) {
          placement_info[i] = stripe.replicas2[j++];
        }
        parity_cnt += lrc->g - global_group_num * b;  // global parities in mixed groups (data + global)
        placement_info[lrc->k + lrc->g + lrc->l - 1] = stripe.replicas2[j++];
        if (ec_schema_.placement_rule == OPTIMAL) {
          if (optimized) {
            parity_cnt += ceil((double)lrc->g / (double)b) - 1;   // local parities in mixed groups
          } else {
            parity_cnt += lrc->l - 1; // all local parities except the global group one
          }
        }
      } else {
        for (int i = lrc->k; i < lrc->k + lrc->g; ++i) {
          placement_info[i] = stripe.replicas2[j++];
        }
        if (!optimized) {
          parity_cnt += lrc->l; // all local parities
        }
      }
    }
  }

  void Coordinator::new_data_placement_for_flat_to_optimal(unsigned int stripe_id,
          const std::vector<unsigned int>& old_placement_info,
          std::vector<unsigned int>& new_placement_info,
          int& data_cnt, int& parity_cnt)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    stripe.ec->placement_rule = OPTIMAL;
    stripe.ec->generate_partition();
    int num_of_partitions = int(stripe.ec->partition_plan.size());
    for (int i = 0; i < num_of_partitions; ++i) {
      int partition_size = int(stripe.ec->partition_plan[i].size());
      int block_idx = stripe.ec->partition_plan[i][0];
      unsigned int node_id = old_placement_info[block_idx];
      new_placement_info[block_idx] = node_id;
      unsigned int cluster_id = node_table_[node_id].map2cluster;
      Cluster &cluster = cluster_table_[cluster_id];
      std::vector<unsigned int> free_nodes;
      for (int j = 0; j < num_of_nodes_per_cluster_; j++) {
        if (node_id != cluster.nodes[j]) {
          free_nodes.push_back(cluster.nodes[j]);
        }
      }
      size_t free_nodes_num = int(free_nodes.size());
      for (int j = 1; j < partition_size; ++j) {
        my_assert(free_nodes_num);
        // randomly select a node
        int node_idx = random_index(free_nodes_num);
        unsigned int node_id = free_nodes[node_idx];
        int block_idx = stripe.ec->partition_plan[i][j];
        new_placement_info[block_idx] = node_id;
        // remove the chosen node from the free list
        auto it_n = std::find(free_nodes.begin(), free_nodes.end(), node_id);
        free_nodes.erase(it_n);
        free_nodes_num--;
        if (block_idx < stripe.ec->k) {
          ++data_cnt;
        }
      }
    }

    if (ec_schema_.ec_type == AZURE_LRC || ec_schema_.ec_type == UNIFORM_CAUCHY_LRC) {
      new_data_placement_for_optimal_to_optimal_lrc(stripe_id, new_placement_info, parity_cnt, true);
      auto lrc = dynamic_cast<LocallyRepairableCode*>(stripe.ec);
      int b = lrc->k / lrc->l;
      if (ec_schema_.ec_type == UNIFORM_CAUCHY_LRC) {
        b = ceil(double(lrc->k + lrc->g) / double(lrc->l));
      }
      if (b <= lrc->g) {
        parity_cnt += lrc->l;
        if (ec_schema_.ec_type == UNIFORM_CAUCHY_LRC) {
          --parity_cnt;
        }
      }
    } else if (ec_schema_.ec_type == PC) {
      new_data_placement_for_optimal_to_optimal_pc(stripe_id, new_placement_info, parity_cnt, true);
      auto pc = dynamic_cast<ProductCode*>(stripe.ec);
      parity_cnt += pc->k1 * pc->m2;
    }
  }

  void Coordinator::new_data_placement_for_flat_to_flat_pc(unsigned int stripe_id,
            std::vector<unsigned int>& placement_info, int& parity_cnt)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    auto pc = dynamic_cast<ProductCode*>(stripe.ec);
    placement_info[pc->k1 * pc->k2] = stripe.replicas2[0];
    parity_cnt += pc->m - 1;
  }

  void Coordinator::new_data_placement_for_optimal_to_optimal_pc(unsigned int stripe_id,
            std::vector<unsigned int>& placement_info, int& parity_cnt, bool optimized)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    auto pc = dynamic_cast<ProductCode*>(stripe.ec);
    int start_idx = pc->k1 * pc->k2;
    int end_idx = (pc->k1 + pc->m1) * pc->k2;
    int rep_idx = 0;
    for (int i = start_idx; i < end_idx; ++i) {
      placement_info[i] = stripe.replicas2[rep_idx++];
    }
    start_idx = (pc->k1 + pc->m1) * pc->k2 + pc->k1 * pc->m2;
    end_idx = (pc->k1 + pc->m1) * (pc->k2 + pc->m2);
    for (int i = start_idx; i < end_idx; ++i) {
      placement_info[i] = stripe.replicas2[rep_idx++];
    }
    if (!optimized) {
      parity_cnt += pc->k1 * pc->m2;
    }
  }

  void Coordinator::new_data_placement_for_flat_to_flat(unsigned int stripe_id,
            std::vector<unsigned int>& placement_info, int& parity_cnt)
  {
    if (ec_schema_.ec_type == AZURE_LRC || ec_schema_.ec_type == UNIFORM_CAUCHY_LRC) {
      new_data_placement_for_flat_to_flat_lrc(stripe_id, placement_info, parity_cnt);
    } else if (ec_schema_.ec_type == PC) {
      new_data_placement_for_flat_to_flat_pc(stripe_id, placement_info, parity_cnt);
    }
  }

  void Coordinator::new_data_placement_for_optimal_to_optimal(unsigned int stripe_id,
            std::vector<unsigned int>& placement_info, int& parity_cnt, bool optimized)
  {
    if (ec_schema_.ec_type == AZURE_LRC || ec_schema_.ec_type == UNIFORM_CAUCHY_LRC) {
      new_data_placement_for_optimal_to_optimal_lrc(stripe_id, placement_info, parity_cnt, optimized);
    } else if (ec_schema_.ec_type == PC) {
      new_data_placement_for_optimal_to_optimal_pc(stripe_id, placement_info, parity_cnt, optimized);
    }
  }

  void Coordinator::generate_encoding_plan_lrc(unsigned int stripe_id,
            std::vector<EncodePlan>& plans,
            const std::vector<unsigned int>& new_placement_info,
            bool optimized)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    auto lrc = dynamic_cast<LocallyRepairableCode*>(stripe.ec);
    int b = lrc->k / lrc->l;
    if (ec_schema_.ec_type == UNIFORM_CAUCHY_LRC) {
      b = ceil(double(lrc->k + lrc->g) / double(lrc->l));
    }
    if (b <= lrc->g && ec_schema_.placement_rule == OPTIMAL && optimized) {
      std::vector<std::vector<int>> groups;
      lrc->grouping_information(groups);
      int group_num = groups.size() - 1;
      if (ec_schema_.ec_type == UNIFORM_CAUCHY_LRC) {
        group_num = groups.size() - 1 - (ceil((double)lrc->g / (double)b) - 1);
      }
      for (int i = 0; i < group_num; ++i) {
        EncodePlan plan;
        plan.block_size = ec_schema_.block_size;
        plan.ec_type = ec_schema_.ec_type;
        stripe.ec->get_coding_parameters(plan.cp);
        plan.partial_encoding = true;
        plan.cluster_id = node_table_[new_placement_info[groups[i][0]]].map2cluster;
        int group_size = groups[i].size();
        for (auto idx : groups[i]) {  // inner cluster
          unsigned int t_node_id = new_placement_info[idx];
          Node& t_node = node_table_[t_node_id];
          if (idx < lrc->k) { // data
            plan.data_block_ids.emplace_back(stripe.block_ids[idx]);
            plan.data_blocks_info.push_back(
                std::make_pair(idx, std::make_pair(t_node.node_ip, t_node.node_port)));
          } else {
            plan.new_parity_block_ids.emplace_back(stripe.block_ids[idx]);
            plan.new_locations.push_back(
                std::make_pair(idx, std::make_pair(t_node.node_ip, t_node.node_port)));
          }
        }
        plans.emplace_back(plan);
      }
      // for global parity group
      EncodePlan plan;
      plan.block_size = ec_schema_.block_size;
      plan.ec_type = ec_schema_.ec_type;
      stripe.ec->get_coding_parameters(plan.cp);
      plan.partial_encoding = true;
      plan.cluster_id = node_table_[stripe.replicas2[0]].map2cluster;
      for (int i = 0; i < lrc->k; ++i) {
        unsigned int t_node_id = stripe.replicas2[i];
        Node& t_node = node_table_[t_node_id];
        plan.data_block_ids.emplace_back(stripe.block_ids[i]);
        plan.data_blocks_info.push_back(
                std::make_pair(i, std::make_pair(t_node.node_ip, t_node.node_port)));
      }
      for (int i = lrc->k; i < lrc->k + lrc->g; ++i) {
        unsigned int t_node_id = new_placement_info[i];
        Node& t_node = node_table_[t_node_id];
        plan.new_parity_block_ids.emplace_back(stripe.block_ids[i]);
        plan.new_locations.push_back(
                std::make_pair(i, std::make_pair(t_node.node_ip, t_node.node_port)));
      }
      if (ec_schema_.ec_type == UNIFORM_CAUCHY_LRC) {
        int last_lid = lrc->k + lrc->g + lrc->l - ceil((double)lrc->g / (double)b);
        for (int idx = last_lid; idx < lrc->k + lrc->g + lrc->l; ++idx) {
          unsigned int t_node_id = new_placement_info[idx];
          Node& t_node = node_table_[t_node_id];
          plan.new_parity_block_ids.emplace_back(stripe.block_ids[idx]);
          plan.new_locations.push_back(
                  std::make_pair(idx, std::make_pair(t_node.node_ip, t_node.node_port)));
        }
      }
      plans.emplace_back(plan);
    } else {
      EncodePlan plan;
      plan.block_size = ec_schema_.block_size;
      plan.ec_type = ec_schema_.ec_type;
      stripe.ec->get_coding_parameters(plan.cp);
      plan.partial_encoding = false;
      plan.cluster_id = node_table_[stripe.replicas2[0]].map2cluster;
      for (int i = 0; i < lrc->k; ++i) {
        unsigned int t_node_id = stripe.replicas2[i];
        Node& t_node = node_table_[t_node_id];
        plan.data_block_ids.emplace_back(stripe.block_ids[i]);
        plan.data_blocks_info.push_back(
                std::make_pair(i, std::make_pair(t_node.node_ip, t_node.node_port)));
      }
      for (int i = lrc->k; i < lrc->k + lrc->g + lrc->l; ++i) {
        unsigned int t_node_id = new_placement_info[i];
        Node& t_node = node_table_[t_node_id];
        plan.new_parity_block_ids.emplace_back(stripe.block_ids[i]);
        plan.new_locations.push_back(
                std::make_pair(i, std::make_pair(t_node.node_ip, t_node.node_port)));
      }
      plans.emplace_back(plan);
    }
  }

  void Coordinator::generate_encoding_plan_pc(unsigned int stripe_id,
            std::vector<EncodePlan>& plans,
            const std::vector<unsigned int>& new_placement_info,
            bool optimized)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    auto pc = dynamic_cast<ProductCode*>(stripe.ec);
    if (ec_schema_.placement_rule == OPTIMAL && optimized) {
      for (int i = 0; i < pc->k1; ++i) {  // for column parity blocks
        EncodePlan plan;
        plan.block_size = ec_schema_.block_size;
        plan.ec_type = RS;
        plan.cp.k = pc->k2;
        plan.cp.m = pc->m2;
        plan.partial_encoding = false;
        plan.cluster_id = node_table_[new_placement_info[i]].map2cluster;
        for (int j = 0; j < pc->k2; ++j) {  // inner cluster
          int idx = j * pc->k1 + i;
          unsigned int t_node_id = new_placement_info[idx];
          Node& t_node = node_table_[t_node_id];
          plan.data_block_ids.emplace_back(stripe.block_ids[idx]);
          plan.data_blocks_info.push_back(
              std::make_pair(j, std::make_pair(t_node.node_ip, t_node.node_port)));
        }
        int base = pc->k + pc->k2 * pc->m1;
        for (int j = 0; j < pc->m2; ++j) {
          int idx = base + j * pc->k1 + i;
          unsigned int t_node_id = new_placement_info[idx];
          Node& t_node = node_table_[t_node_id];
          plan.new_parity_block_ids.emplace_back(stripe.block_ids[idx]);
          plan.new_locations.push_back(
              std::make_pair(j + pc->k2, std::make_pair(t_node.node_ip, t_node.node_port)));
        }
        plans.emplace_back(plan);
      }
      // for m1 columns of row parity blocks
      EncodePlan plan;
      plan.block_size = ec_schema_.block_size;
      plan.ec_type = ec_schema_.ec_type;
      stripe.ec->get_coding_parameters(plan.cp);
      plan.partial_encoding = false;
      plan.cluster_id = node_table_[stripe.replicas2[0]].map2cluster;
      for (int i = 0; i < pc->k; ++i) {
        unsigned int t_node_id = stripe.replicas2[i];
        Node& t_node = node_table_[t_node_id];
        plan.data_block_ids.emplace_back(stripe.block_ids[i]);
        plan.data_blocks_info.push_back(
                std::make_pair(i, std::make_pair(t_node.node_ip, t_node.node_port)));
      }
      for (int i = pc->k; i < pc->k + pc->m1 * pc->k2; ++i) {
        unsigned int t_node_id = new_placement_info[i];
        Node& t_node = node_table_[t_node_id];
        plan.new_parity_block_ids.emplace_back(stripe.block_ids[i]);
        plan.new_locations.push_back(
                std::make_pair(i, std::make_pair(t_node.node_ip, t_node.node_port)));
      }
      for (int i = (pc->k1 + pc->m1) * pc->k2 + pc->k1 * pc->m2; i < pc->k + pc->m; ++i) {
        unsigned int t_node_id = new_placement_info[i];
        Node& t_node = node_table_[t_node_id];
        plan.new_parity_block_ids.emplace_back(stripe.block_ids[i]);
        plan.new_locations.push_back(
                std::make_pair(i, std::make_pair(t_node.node_ip, t_node.node_port)));
      }
      plans.emplace_back(plan);
    } else { // encode all the parity blocks
      EncodePlan plan;
      plan.block_size = ec_schema_.block_size;
      plan.ec_type = ec_schema_.ec_type;
      stripe.ec->get_coding_parameters(plan.cp);
      plan.partial_encoding = false;
      plan.cluster_id = node_table_[stripe.replicas2[0]].map2cluster;
      for (int i = 0; i < pc->k; ++i) {
        unsigned int t_node_id = stripe.replicas2[i];
        Node& t_node = node_table_[t_node_id];
        plan.data_block_ids.emplace_back(stripe.block_ids[i]);
        plan.data_blocks_info.push_back(
                std::make_pair(i, std::make_pair(t_node.node_ip, t_node.node_port)));
      }
      for (int i = pc->k; i < pc->k + pc->m; ++i) {
        unsigned int t_node_id = new_placement_info[i];
        Node& t_node = node_table_[t_node_id];
        plan.new_parity_block_ids.emplace_back(stripe.block_ids[i]);
        plan.new_locations.push_back(
                std::make_pair(i, std::make_pair(t_node.node_ip, t_node.node_port)));
      }
      plans.emplace_back(plan);
    }
  }
}