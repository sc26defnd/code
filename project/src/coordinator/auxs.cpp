#include "coordinator.h"

namespace ECProject
{
  void Coordinator::write_logs(Logger::LogLevel level, std::string& msg)
  {
    if (level != Logger::LogLevel::DEBUG) {
      msg = "[Coordinator] " + msg;
    }
    if (IF_LOG_TO_FILE) {
      logger_->log(level, msg);
    } else {
      printf("%s", msg.c_str());
    }
  }

  void Coordinator::init_ec_schema(std::string config_file)
  {
    ParametersInfo paras;
    parse_args(logger_, paras, config_file);
    set_erasure_coding_parameters(paras);
  }
  
  void Coordinator::init_cluster_info()
  {
    tinyxml2::XMLDocument xml;
    xml.LoadFile(xml_path_.c_str());
    tinyxml2::XMLElement *root = xml.RootElement();
    unsigned int node_id = 0;
    num_of_clusters_ = 0;

    for (tinyxml2::XMLElement *cluster = root->FirstChildElement(); 
          cluster != nullptr; cluster = cluster->NextSiblingElement()) 
    {
      unsigned int cluster_id(std::stoi(cluster->Attribute("id")));
      std::string proxy(cluster->Attribute("proxy"));

      cluster_table_[cluster_id].cluster_id = cluster_id;
      auto pos = proxy.find(':');
      cluster_table_[cluster_id].proxy_ip = proxy.substr(0, pos);
      cluster_table_[cluster_id].proxy_port = std::stoi(proxy.substr(pos + 1, proxy.size()));

      num_of_nodes_per_cluster_ = 0;
      for (tinyxml2::XMLElement *node = cluster->FirstChildElement()->FirstChildElement();
            node != nullptr; node = node->NextSiblingElement()) 
      {
        cluster_table_[cluster_id].nodes.push_back(node_id);

        std::string node_uri(node->Attribute("uri"));
        node_table_[node_id].node_id = node_id;
        auto pos = node_uri.find(':');
        node_table_[node_id].node_ip = node_uri.substr(0, pos);
        node_table_[node_id].node_port = std::stoi(node_uri.substr(pos + 1, node_uri.size()));
        node_table_[node_id].map2cluster = cluster_id;

        //node_table_[node_id].zone_id = num_of_nodes_per_cluster_ % ec_schema_.zones_per_rack;

        node_id++;
        num_of_nodes_per_cluster_++;
      }
      num_of_clusters_++;
    }
  }

  void Coordinator::init_proxy_info()
  {
    for (auto cur = cluster_table_.begin(); cur != cluster_table_.end(); cur++) {
      std::string proxy_ip = cur->second.proxy_ip;
      int proxy_port = cur->second.proxy_port;
      std::string location = proxy_ip + std::to_string(proxy_port);
      my_assert(proxies_.contains(location) == false);

      proxies_[location] = std::make_unique<coro_rpc::coro_rpc_client>();
      if (!IF_SIMULATION) {
        async_simple::coro::syncAwait(proxies_[location]->connect(proxy_ip, std::to_string(proxy_port)));
        auto msg = async_simple::coro::syncAwait(proxies_[location]->call<&Proxy::checkalive>("hello"));
        if (msg != "hello") {
          std::string msg = "[Proxy Check] failed to connect " + location + "\n";
          write_logs(Logger::LogLevel::ERROR, msg);
        }
      }
    }
  }

  void Coordinator::reset_metadata()
  {
    cur_stripe_id_ = 0;
    cur_block_id_ = 0;
    time_ = 0;
    merged_flag_ = false;
    commited_object_table_.clear();
    updating_object_table_.clear();
    merge_groups_.clear();
    free_clusters_.clear();
    for (auto& kv : stripe_table_) {
      if (kv.second.ec != nullptr) {
        delete kv.second.ec;
        kv.second.ec = nullptr;
      }
    }
    stripe_table_.clear();
    for (auto &kv : cluster_table_) {
      kv.second.holding_stripe_ids.clear();
    }
  }

  Stripe& Coordinator::new_stripe(size_t block_size, ErasureCode *ec)
  {
    my_assert(ec != nullptr);
    Stripe temp;
    temp.stripe_id = cur_stripe_id_++;
    temp.block_size = block_size;
    stripe_table_[temp.stripe_id] = temp;
    stripe_table_[temp.stripe_id].ec = clone_ec(ec_schema_.ec_type, ec);
    return stripe_table_[temp.stripe_id];
  }

  ErasureCode* Coordinator::new_ec_for_merge(int step_size)
  {
    CodingParameters cp;
    ec_schema_.ec->get_coding_parameters(cp);
    ECFAMILY ec_family = check_ec_family(ec_schema_.ec_type);
    if (ec_family == RSCodes) {
      cp.k *= step_size;
    } else if (ec_family == LRCs) {
      cp.k *= step_size;
      cp.l *= step_size;
    } else {
      if (ec_schema_.multistripe_placement_rule == VERTICAL) {
        cp.k2 *= step_size;
      } else {
        cp.k1 *= step_size;
      }
    }
    return ec_factory(ec_schema_.ec_type, cp);
  }

  void Coordinator::init_placement_info(PlacementInfo &placement, std::string key,
                                        size_t value_len, size_t block_size, bool isreplica)
  {
    placement.ec_type = ec_schema_.ec_type;
    placement.key = key;
    placement.value_len = value_len;
    ec_schema_.ec->get_coding_parameters(placement.cp);
    placement.block_size = block_size;
    placement.cp.x = ec_schema_.x;
    placement.from_two_replica = ec_schema_.from_two_replica;
    placement.isreplica = isreplica;
    placement.merged_flag = merged_flag_;
    if (ec_schema_.multistripe_placement_rule == VERTICAL) {
      placement.isvertical = true;
    }
    placement.read_mode = ec_schema_.read_mode;
  }

  void Coordinator::find_out_stripe_partitions(unsigned int stripe_id)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    stripe.ec->partition_plan.clear();
    std::unordered_map<unsigned int, std::vector<int>> blocks_in_clusters;
    for (int i = 0; i < stripe.ec->k + stripe.ec->m; i++) {
      unsigned int node_id = stripe.blocks2nodes[i];
      unsigned int cluster_id = node_table_[node_id].map2cluster;
      if (blocks_in_clusters.find(cluster_id) == blocks_in_clusters.end()) {
        blocks_in_clusters[cluster_id] = std::vector<int>({i});
      } else {
        blocks_in_clusters[cluster_id].push_back(i);
      }
    }
    for (auto& kv : blocks_in_clusters) {
      stripe.ec->partition_plan.push_back(kv.second);
    }
    if (IF_DEBUG) {
      auto msg = stripe.ec->print_info(stripe.ec->partition_plan, "partition");
      write_logs(Logger::LogLevel::DEBUG, msg);
    }
  }

  bool Coordinator::if_subject_to_fault_tolerance_lrc(
          ErasureCode *ec, std::vector<int> blocks_in_cluster,
          std::unordered_map<int, std::vector<int>>& group_blocks)
  {
    auto lrc = dynamic_cast<LocallyRepairableCode*>(ec);
    int blocks_num = (int)blocks_in_cluster.size();
    for (int i = 0; i < blocks_num; i++) {
      int gid = lrc->bid2gid(blocks_in_cluster[i]);
      if (group_blocks.find(gid) == group_blocks.end()) {
        group_blocks[gid] = std::vector({blocks_in_cluster[i]});
      } else {
        group_blocks[gid].push_back(blocks_in_cluster[i]);
      }
    }
    int group_num = (int)group_blocks.size();
    if (blocks_num > group_num + lrc->g) {
      return false;
    }
    return true;
  }

  bool Coordinator::if_subject_to_fault_tolerance_pc(
            ErasureCode *ec, std::vector<int> blocks_in_cluster,
            std::unordered_map<int, std::vector<int>> &col_blocks)
  {
    auto pc = dynamic_cast<ProductCode*>(ec);
    int blocks_num = (int)blocks_in_cluster.size();
    for (int i = 0; i < blocks_num; i++) {
      int row = -1, col = -1;
      pc->bid2rowcol(blocks_in_cluster[i], row, col);
      if (col_blocks.find(col) == col_blocks.end()) {
        col_blocks[col] = std::vector({blocks_in_cluster[i]});
      } else {
        col_blocks[col].push_back(blocks_in_cluster[i]);
      }
    }
    int col_num = (int)col_blocks.size();
    if (col_num > pc->m1) {
      return false;
    }
    return true;
  }
}
