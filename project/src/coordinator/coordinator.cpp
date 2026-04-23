#include "coordinator.h"

namespace ECProject
{
  Coordinator::Coordinator(std::string ip, int port, std::string xml_path)
      : ip_(ip), port_(port), xml_path_(xml_path)
  {
    easylog::set_min_severity(easylog::Severity::ERROR);
    rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(4, port_);
    rpc_server_->register_handler<&Coordinator::checkalive>(this);
    rpc_server_->register_handler<&Coordinator::set_erasure_coding_parameters>(this);
    rpc_server_->register_handler<&Coordinator::request_set>(this);
    rpc_server_->register_handler<&Coordinator::commit_object>(this);
    rpc_server_->register_handler<&Coordinator::request_get>(this);
    rpc_server_->register_handler<&Coordinator::request_delete_by_stripe>(this);
    rpc_server_->register_handler<&Coordinator::request_repair>(this);
    rpc_server_->register_handler<&Coordinator::request_merge>(this);
    rpc_server_->register_handler<&Coordinator::request_scale>(this);
    rpc_server_->register_handler<&Coordinator::request_redundancy_transition>(this);
    rpc_server_->register_handler<&Coordinator::list_stripes>(this);

    cur_stripe_id_ = 0;
    cur_block_id_ = 0;
    time_ = 0;
    
    if (IF_LOG_TO_FILE) {
      std::string logdir = "./log/";
      if (access(logdir.c_str(), 0) == -1) {
        mkdir(logdir.c_str(), S_IRWXU);
      }
      logger_ = new Logger(logdir + "coordinator-" + getStartTime() + ".log");
    }

    init_cluster_info();
    init_proxy_info();

    if (IF_DEBUG) {
      std::string msg = "Start the coordinator! " + ip + ":" + std::to_string(port) + "\n";
      write_logs(Logger::LogLevel::INFO, msg);
    }
  }
  Coordinator::~Coordinator()
  { 
    rpc_server_->stop();
    if (logger_ != nullptr) {
      delete logger_;
      logger_ = nullptr;
    }
  }

  void Coordinator::run() { auto err = rpc_server_->start(); }

  std::string Coordinator::checkalive(std::string msg) 
  { 
    return msg + " Hello, it's me. The coordinator!"; 
  }

  void Coordinator::set_erasure_coding_parameters(ParametersInfo paras)
  {
    ec_schema_.partial_decoding = paras.partial_decoding;
    ec_schema_.partial_scheme = paras.partial_scheme;
    ec_schema_.repair_priority = paras.repair_priority;
    ec_schema_.repair_method = paras.repair_method;
    ec_schema_.from_two_replica = paras.from_two_replica;
    ec_schema_.is_ec_now = paras.is_ec_now;
    ec_schema_.if_zone_aware = paras.if_zone_aware;
    ec_schema_.ec_type = paras.ec_type;
    ec_schema_.placement_rule = paras.placement_rule;
    ec_schema_.placement_rule_for_trans = paras.placement_rule_for_trans;
    ec_schema_.multistripe_placement_rule = paras.multistripe_placement_rule;
    ec_schema_.x = paras.cp.x;
    ec_schema_.block_size = paras.block_size;
    ec_schema_.zones_per_rack = paras.zones_per_rack;
    ec_schema_.read_mode = paras.read_mode;
    ec_schema_.ec = ec_factory(paras.ec_type, paras.cp);
    reset_metadata();

    if (ec_schema_.if_zone_aware) { 
        for (auto& cluster_pair : cluster_table_) {
            int node_idx_in_cluster = 0;
            for (unsigned int nid : cluster_pair.second.nodes) {
                // 正式分配 Zone ID！
                node_table_[nid].zone_id = node_idx_in_cluster % ec_schema_.zones_per_rack;
                node_idx_in_cluster++;
            }
        }
    }

    if (IF_DEBUG) {
      if(ec_schema_.if_zone_aware){
        std::string topo_msg = "\n[DEBUG] === Zone Assignment Completed! ===\n";
        topo_msg += "Received zones_per_rack from Client: " + std::to_string(ec_schema_.zones_per_rack) + "\n";
        for (unsigned int cid = 0; cid < num_of_clusters_; ++cid) {
            topo_msg += "Cluster " + std::to_string(cid) + ": ";
            for (unsigned int nid : cluster_table_[cid].nodes) {
                topo_msg += "N" + std::to_string(nid) + "(Z" + std::to_string(node_table_[nid].zone_id) + ") ";
            }
            topo_msg += "\n";
        }
        write_logs(Logger::LogLevel::INFO, topo_msg);
      } else{
        std::string topo_msg = "\n[DEBUG] Cluster assignment \n";
        for (unsigned int cid = 0; cid < num_of_clusters_; ++cid) {
            topo_msg += "Cluster " + std::to_string(cid) + ": ";
            for (unsigned int nid : cluster_table_[cid].nodes) {
                topo_msg += "N" + std::to_string(nid) + " ";
            }
            topo_msg += "\n";
        }
        write_logs(Logger::LogLevel::INFO, topo_msg);
      }   
        
    }
  }

  SetResp Coordinator::request_set(
              std::vector<std::string> object_keys,
              std::vector<size_t> object_sizes,
              std::vector<unsigned int> object_accessrates)
  {
    int num_of_objects = (int)object_keys.size();
    my_assert(num_of_objects > 0);
    mutex_.lock();
    for (int i = 0; i < num_of_objects; i++) {
      std::string key = object_keys[i];
      if (commited_object_table_.contains(key)) {
        mutex_.unlock();
        my_assert(false);
      }
    }
    mutex_.unlock();

    size_t total_value_len = 0;
    std::vector<ObjectInfo> new_objects;
    for (int i = 0; i < num_of_objects; i++) {
      ObjectInfo new_object;
      new_object.value_len = object_sizes[i];
      new_object.access_rate = object_accessrates[i];
      new_object.access_cnt = 0;
      my_assert(new_object.value_len % ec_schema_.block_size == 0);
      total_value_len += new_object.value_len;
      new_objects.push_back(new_object);
    }

    if (IF_DEBUG) {
      std::string msg = "[Set] Start to process " + std::to_string(num_of_objects)
                        + " objects. With total length of "
                        + std::to_string(total_value_len) + ".\n";
      write_logs(Logger::LogLevel::INFO, msg);
    }

    my_assert(total_value_len % (ec_schema_.block_size * ec_schema_.ec->k) == 0);
    PlacementInfo placement;
    unsigned int selected_node_id = 0;

    if (ec_schema_.is_ec_now) {
      init_placement_info(placement, object_keys[0],
                          total_value_len, ec_schema_.block_size, false);

      int num_of_stripes = total_value_len / (ec_schema_.ec->k * ec_schema_.block_size);
      size_t left_value_len = total_value_len;
      int cumulative_len = 0, obj_idx = 0;
      for (int i = 0; i < num_of_stripes; i++) {
        left_value_len -= ec_schema_.ec->k * ec_schema_.block_size;
        Stripe& stripe = new_stripe(ec_schema_.block_size, ec_schema_.ec);
        if (ec_schema_.ec_type == ECTYPE::NON_UNIFORM_LRC) {
          auto lrc = dynamic_cast<Non_Uni_LRC*>(stripe.ec);
          lrc->generate_coding_parameters_for_a_stripe(object_sizes,
            object_accessrates, ec_schema_.block_size);
        }

        cumulative_len = 0;
        while (cumulative_len < ec_schema_.ec->k * ec_schema_.block_size) {
          stripe.objects.push_back(object_keys[obj_idx]);
          new_objects[obj_idx].map2stripe = stripe.stripe_id;
          cumulative_len += object_sizes[obj_idx];
          obj_idx++;
        }
        my_assert(cumulative_len == ec_schema_.ec->k * ec_schema_.block_size);
        stripe.cur_block_num = ec_schema_.ec->k + ec_schema_.ec->m;

        if (IF_DEBUG) {
          std::string msg = "[Set] Ready to data placement for stripe "
                            + std::to_string(stripe.stripe_id) + "\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }

        generate_placement_ec(stripe.stripe_id);

        if (IF_DEBUG) {
          std::string msg = "[Set] Finish data placement for stripe "
                            + std::to_string(stripe.stripe_id) + "\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }

        placement.stripe_ids.push_back(stripe.stripe_id);
        for (auto& node_id : stripe.blocks2nodes) {
          auto& node = node_table_[node_id];
          placement.datanode_ip_port.push_back(std::make_pair(node.map2cluster,
            std::make_pair(node.node_ip, node.node_port)));
        }
        for (auto& block_id : stripe.block_ids) {
          placement.block_ids.push_back(block_id);
        }
        selected_node_id = stripe.blocks2nodes[0];
      }
      my_assert(left_value_len == 0);
    } else {
      init_placement_info(placement, object_keys[0],
                          total_value_len, ec_schema_.block_size, true);
      
      int num_of_stripes = total_value_len / (ec_schema_.ec->k * ec_schema_.block_size);
      size_t left_value_len = total_value_len;
      int cumulative_len = 0, obj_idx = 0;
      for (int i = 0; i < num_of_stripes; i++) {
        left_value_len -= ec_schema_.ec->k * ec_schema_.block_size;
        Stripe& stripe = new_stripe(ec_schema_.block_size, ec_schema_.ec);
        if (ec_schema_.ec_type == ECTYPE::NON_UNIFORM_LRC) {
          auto lrc = dynamic_cast<Non_Uni_LRC*>(stripe.ec);
          lrc->generate_coding_parameters_for_a_stripe(object_sizes,
            object_accessrates, ec_schema_.block_size);
        }

        cumulative_len = 0;
        while (cumulative_len < ec_schema_.ec->k * ec_schema_.block_size) {
          stripe.objects.push_back(object_keys[obj_idx]);
          new_objects[obj_idx].map2stripe = stripe.stripe_id;
          cumulative_len += object_sizes[obj_idx];
          obj_idx++;
        }
        my_assert(cumulative_len == ec_schema_.ec->k * ec_schema_.block_size);
        stripe.cur_block_num = ec_schema_.ec->k;

        if (IF_DEBUG) {
          std::string msg = "[Set] Ready to data placement for stripe "
                            + std::to_string(stripe.stripe_id) + "\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }

        generate_placement_replica(stripe.stripe_id);
        if (IF_DEBUG) {
          std::string msg = "[Set] Finish data placement for stripe "
                            + std::to_string(stripe.stripe_id) + "\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }

        placement.stripe_ids.push_back(stripe.stripe_id);

        // replica 1
        for (int i = 0; i < stripe.ec->k; ++i) {
          unsigned int node_id = stripe.blocks2nodes[i];
          auto& node = node_table_[node_id];
          placement.datanode_ip_port.push_back(std::make_pair(node.map2cluster,
            std::make_pair(node.node_ip, node.node_port)));
        }
        // replica 2
        for (int i = 0; i < stripe.ec->k; ++i) {
          unsigned int node_id = stripe.replicas2[i];
          auto& node = node_table_[node_id];
          placement.datanode_ip_port.push_back(std::make_pair(node.map2cluster,
            std::make_pair(node.node_ip, node.node_port)));
        }
        // replica 3
        if (!ec_schema_.from_two_replica) {
          for (int i = 0; i < stripe.ec->k; ++i) {
            unsigned int node_id = stripe.replicas3[i];
            auto& node = node_table_[node_id];
            placement.datanode_ip_port.push_back(std::make_pair(node.map2cluster,
              std::make_pair(node.node_ip, node.node_port)));
          }
        }
        for (int i = 0; i < stripe.ec->k; ++i) {
          placement.block_ids.push_back(stripe.block_ids[i]);
        }
        selected_node_id = stripe.replicas2[0];
      }
    }

    mutex_.lock();
    for (int i = 0; i < num_of_objects; i++) {
      updating_object_table_[object_keys[i]] = new_objects[i];
    }
    mutex_.unlock();

    unsigned int selected_cluster_id = node_table_[selected_node_id].map2cluster;
    std::string selected_proxy_ip = cluster_table_[selected_cluster_id].proxy_ip;
    int selected_proxy_port = cluster_table_[selected_cluster_id].proxy_port;

    if (IF_DEBUG) {
      std::string msg = "[Set] Select proxy " + selected_proxy_ip + ":"
                        + std::to_string(selected_proxy_port) + " in cluster "
                        + std::to_string(selected_cluster_id)
                        + " to handle set!" + "\n";
      write_logs(Logger::LogLevel::INFO, msg);
    }

    if (IF_SIMULATION) {// simulation, commit object
      mutex_.lock();
      for (int i = 0; i < num_of_objects; i++) {
        my_assert(commited_object_table_.contains(object_keys[i]) == false &&
                  updating_object_table_.contains(object_keys[i]) == true);
        commited_object_table_[object_keys[i]] = updating_object_table_[object_keys[i]];
        updating_object_table_.erase(object_keys[i]);
      }
      mutex_.unlock();
    } else {
      async_simple::coro::syncAwait(
        proxies_[selected_proxy_ip + std::to_string(selected_proxy_port)]
            ->call<&Proxy::encode_and_store_object>(placement));
    }

    SetResp response;
    response.proxy_ip = selected_proxy_ip;
    response.proxy_port = selected_proxy_port + SOCKET_PORT_OFFSET; // port for transfer data
    return response;
  }

  void Coordinator::commit_object(std::vector<std::string> keys, bool commit)
  {
    int num = (int)keys.size();
    if (commit) {
      mutex_.lock();
      std::unordered_set<unsigned int> stripe_set;
      for (int i = 0; i < num; i++) {
        my_assert(commited_object_table_.contains(keys[i]) == false &&
                  updating_object_table_.contains(keys[i]) == true);
        commited_object_table_[keys[i]] = updating_object_table_[keys[i]];
        updating_object_table_.erase(keys[i]);
        stripe_set.insert(commited_object_table_[keys[i]].map2stripe);
      }
      if (ec_schema_.is_ec_now) {
        for (auto& stripe_id : stripe_set) {
          Stripe& stripe = stripe_table_[stripe_id];
          for (auto& nid : stripe.blocks2nodes) {
            unsigned int cid = node_table_[nid].map2cluster;
            cluster_table_[cid].holding_stripe_ids.insert(stripe_id);
          }
        }
      } else {
        for (auto& stripe_id : stripe_set) {
          Stripe& stripe = stripe_table_[stripe_id];
          for (auto i = 0; i < stripe.ec->k; i++) {
            unsigned int nid = stripe.blocks2nodes[i];
            unsigned int cid = node_table_[nid].map2cluster;
            cluster_table_[cid].holding_stripe_ids.insert(stripe_id);
          }
        }
      }
      mutex_.unlock();
    } else {
      for (int i = 0; i < num; i++) {
        ObjectInfo& objects = updating_object_table_[keys[i]];
        int stripe_id = objects.map2stripe;
        for (auto it = cluster_table_.begin(); it != cluster_table_.end(); it++) {
          Cluster& cluster = it->second;
          cluster.holding_stripe_ids.erase(stripe_id);
        }
        if (stripe_table_.find(stripe_id) != stripe_table_.end()) {
          stripe_table_.erase(stripe_id);
          cur_stripe_id_--;
        }
      }
      mutex_.lock();
      for (int i = 0; i < num; i++) {
        my_assert(commited_object_table_.contains(keys[i]) == false &&
                  updating_object_table_.contains(keys[i]) == true);
        updating_object_table_.erase(keys[i]);
      }
      mutex_.unlock();
    }
  }

  size_t Coordinator::request_get(std::string key, std::string client_ip,
                                  int client_port)
  {
    mutex_.lock();
    if (commited_object_table_.contains(key) == false) {
      mutex_.unlock();
      my_assert(false);
    }
    ObjectInfo &object = commited_object_table_[key];
    mutex_.unlock();

    PlacementInfo placement;
  
    unsigned int stripe_id = object.map2stripe;
    Stripe &stripe = stripe_table_[stripe_id];
    init_placement_info(placement, key, object.value_len, stripe.block_size, !ec_schema_.is_ec_now);

    placement.stripe_ids.push_back(stripe_id);

    int num_of_object_in_a_stripe = (int)stripe.objects.size();
    int offset = 0;
    for (int i = 0; i < num_of_object_in_a_stripe; i++) {
      if (stripe.objects[i] != key) {
        int t_object_len = commited_object_table_[stripe.objects[i]].value_len;
        offset += t_object_len / stripe.block_size;     // must be block_size of stripe
      } else {
        commited_object_table_[stripe.objects[i]].access_cnt++;
        if (merged_flag_) {
          placement.cp.seri_num = i;
          placement.cp.x = num_of_object_in_a_stripe;
        }
        break;
      }
    }
    placement.offset = offset;

    unsigned int selected_node_id = 0;
    if (ec_schema_.is_ec_now) {
      for (auto& node_id : stripe.blocks2nodes) {
        auto& node = node_table_[node_id];
        placement.datanode_ip_port.push_back(std::make_pair(node.map2cluster,
            std::make_pair(node.node_ip, node.node_port)));
      }
      for (auto& block_id : stripe.block_ids) {
        placement.block_ids.push_back(block_id);
      }
      selected_node_id = stripe.blocks2nodes[offset];
    } else {
      // replica 2
      for (int i = 0; i < stripe.ec->k; ++i) {
        unsigned int node_id = stripe.replicas2[i];
        auto& node = node_table_[node_id];
        placement.datanode_ip_port.push_back(std::make_pair(node.map2cluster,
          std::make_pair(node.node_ip, node.node_port)));
      }
      // replica 1
      for (int i = 0; i < stripe.ec->k; ++i) {
        unsigned int node_id = stripe.blocks2nodes[i];
        auto& node = node_table_[node_id];
        placement.datanode_ip_port.push_back(std::make_pair(node.map2cluster,
          std::make_pair(node.node_ip, node.node_port)));
      }
      // replica 3
      if (!ec_schema_.from_two_replica) {
        for (int i = 0; i < stripe.ec->k; ++i) {
          unsigned int node_id = stripe.replicas3[i];
          auto& node = node_table_[node_id];
          placement.datanode_ip_port.push_back(std::make_pair(node.map2cluster,
            std::make_pair(node.node_ip, node.node_port)));
        }
      }
      for (int i = 0; i < stripe.ec->k; ++i) {
        placement.block_ids.push_back(stripe.block_ids[i]);
      }
      selected_node_id = stripe.replicas2[0];
    }

    if (!IF_SIMULATION) {
      placement.client_ip = client_ip;
      placement.client_port = client_port;
      int selected_cluster_id = node_table_[selected_node_id].map2cluster;
      std::string location = cluster_table_[selected_cluster_id].proxy_ip +
          std::to_string(cluster_table_[selected_cluster_id].proxy_port);
      if (IF_DEBUG) {
        std::string msg = "[Get] Select proxy "
                          + cluster_table_[selected_cluster_id].proxy_ip + ":"
                          + std::to_string(cluster_table_[selected_cluster_id].proxy_port)
                          + " in cluster "
                          + std::to_string(cluster_table_[selected_cluster_id].cluster_id)
                          + " to handle get!" + "\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }
      async_simple::coro::syncAwait(
          proxies_[location]->call<&Proxy::decode_and_get_object>(placement));
    }

    return object.value_len;
  }

  void Coordinator::request_delete_by_stripe(std::vector<unsigned int> stripe_ids)
  {
    std::unordered_set<std::string> objects_key;
    int num_of_stripes = (int)stripe_ids.size();
    if (IF_DEBUG) {
      std::string msg = "[Del] delete " + std::to_string(num_of_stripes)
                        + " stripes!\n";
      write_logs(Logger::LogLevel::INFO, msg);
    }
    DeletePlan delete_info;
    for (int i = 0; i < num_of_stripes; i++) {
      auto &stripe = stripe_table_[stripe_ids[i]];
      for (auto key : stripe.objects) {
        objects_key.insert(key);
      }
      int n = stripe.ec->k + stripe.ec->m;
      my_assert(n == (int)stripe.blocks2nodes.size());
      for (int j = 0; j < n; j++) {
        delete_info.block_ids.push_back(stripe.block_ids[j]);
        auto &node = node_table_[stripe.blocks2nodes[j]];
        delete_info.blocks_info.push_back({node.node_ip, node.node_port});
      }
    }

    if (!IF_SIMULATION) {
      int selected_proxy_id = random_index(cluster_table_.size());
      std::string location = cluster_table_[selected_proxy_id].proxy_ip +
          std::to_string(cluster_table_[selected_proxy_id].proxy_port);
      async_simple::coro::syncAwait(
          proxies_[location]->call<&Proxy::delete_blocks>(delete_info));
    }

    // update meta data
    for (int i = 0; i < num_of_stripes; i++) {
      for (auto it = cluster_table_.begin(); it != cluster_table_.end(); it++) {
        Cluster& cluster = it->second;
        cluster.holding_stripe_ids.erase(stripe_ids[i]);
      }
      stripe_table_.erase(stripe_ids[i]);
    }
    mutex_.lock();
    for (auto key : objects_key) {
      commited_object_table_.erase(key);
    }
    mutex_.unlock();
    if (IF_DEBUG) {
      std::string msg = "[Del] Finish delete!\n";
      write_logs(Logger::LogLevel::INFO, msg);
    }
  }

  std::vector<unsigned int> Coordinator::list_stripes()
  {
    std::vector<unsigned int> stripe_ids;
    for (auto it = stripe_table_.begin(); it != stripe_table_.end(); it++) {
      stripe_ids.push_back(it->first);
    }
    return stripe_ids;
  }

  RepairResp Coordinator::request_repair(std::vector<unsigned int> failed_ids, int stripe_id, int maintenance_type)
  {
    RepairResp response;
    do_repair(failed_ids, stripe_id, response, maintenance_type);
    return response;
  }

  MergeResp Coordinator::request_merge(int step_size)
  {
    my_assert(step_size == ec_schema_.x && !merged_flag_);
    MergeResp response;
    do_stripe_merging(response, step_size);
    return response;
  }

  ScaleResp Coordinator::request_scale(float storage_overhead_upper, float gamma)
  {
    ScaleResp response;
    // todo
    return response;
  }

  TransResp Coordinator::request_redundancy_transition(bool optimized)
  {
    TransResp response;
    do_redundancy_transition(response, optimized);
    return response;
  }
}
