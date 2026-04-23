#include "proxy.h"

namespace ECProject
{
  Proxy::Proxy(std::string ip, int port, std::string networkcore,
               std::string config_path, std::string logfile)
      : ip_(ip), port_(port), networkcore_(networkcore), config_path_(config_path),
        port_for_transfer_data_(port + SOCKET_PORT_OFFSET),
        acceptor_(io_context_, asio::ip::tcp::endpoint(asio::ip::address::from_string(ip.c_str()), port + SOCKET_PORT_OFFSET))
  {
    easylog::set_min_severity(easylog::Severity::ERROR);
    // port is for rpc, port + SOCKET_PORT_OFFSET is for socket
    rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(1, port_);
    rpc_server_->register_handler<&Proxy::checkalive>(this);
    rpc_server_->register_handler<&Proxy::encode_and_store_object>(this);
    rpc_server_->register_handler<&Proxy::decode_and_get_object>(this);
    rpc_server_->register_handler<&Proxy::encode_stripe>(this);
    rpc_server_->register_handler<&Proxy::delete_blocks>(this);
    rpc_server_->register_handler<&Proxy::main_repair>(this);
    rpc_server_->register_handler<&Proxy::help_repair>(this);
    rpc_server_->register_handler<&Proxy::main_recal>(this);
    rpc_server_->register_handler<&Proxy::help_recal>(this);
    rpc_server_->register_handler<&Proxy::block_relocation>(this);

    init_datanodes();
    if (IF_LOG_TO_FILE)
    {
      std::string logdir = "./log/";
      if (access(logdir.c_str(), 0) == -1)
      {
        mkdir(logdir.c_str(), S_IRWXU);
      }
      if (logfile != "")
      {
        logger_ = new Logger(logdir + logfile);
      }
      else
      {
        logger_ = new Logger(logdir + "proxy-" + getStartTime() + ".log");
      }
    }
  }

  Proxy::~Proxy()
  {
    acceptor_.close();
    rpc_server_->stop();
    if (logger_ != nullptr)
    {
      delete logger_;
      logger_ = nullptr;
    }
  }

  void Proxy::run() { auto err = rpc_server_->start(); }

  std::string Proxy::checkalive(std::string msg)
  {
    return msg;
  }

  void Proxy::write_logs(Logger::LogLevel level, std::string &msg)
  {
    if (level != Logger::LogLevel::DEBUG)
    {
      msg = "[Proxy" + std::to_string(self_cluster_id_) + "]" + msg;
    }
    if (IF_LOG_TO_FILE)
    {
      logger_->log(level, msg);
    }
    else
    {
      printf("%s", msg.c_str());
    }
  }

  void Proxy::init_datanodes()
  {
    tinyxml2::XMLDocument xml;
    xml.LoadFile(config_path_.c_str());
    tinyxml2::XMLElement *root = xml.RootElement();
    for (tinyxml2::XMLElement *cluster = root->FirstChildElement();
         cluster != nullptr; cluster = cluster->NextSiblingElement())
    {
      std::string cluster_id(cluster->Attribute("id"));
      std::string proxy(cluster->Attribute("proxy"));
      if (proxy == ip_ + ":" + std::to_string(port_))
      {
        self_cluster_id_ = std::stoi(cluster_id);
      }
      for (tinyxml2::XMLElement *node = cluster->FirstChildElement()->FirstChildElement();
           node != nullptr; node = node->NextSiblingElement())
      {
        std::string node_uri(node->Attribute("uri"));
        datanodes_[node_uri] = std::make_unique<coro_rpc::coro_rpc_client>();
        std::string ip = node_uri.substr(0, node_uri.find(':'));
        int port = std::stoi(node_uri.substr(node_uri.find(':') + 1, node_uri.size()));
        async_simple::coro::syncAwait(
            datanodes_[node_uri]->connect(ip, std::to_string(port)));
      }
    }
    // init networkcore
    datanodes_[networkcore_] = std::make_unique<coro_rpc::coro_rpc_client>();
    std::string ip = networkcore_.substr(0, networkcore_.find(':'));
    int port = std::stoi(networkcore_.substr(networkcore_.find(':') + 1,
                                             networkcore_.size()));
    async_simple::coro::syncAwait(
        datanodes_[networkcore_]->connect(ip, std::to_string(port)));
  }

  void Proxy::write_to_datanode(const char *key, size_t key_len,
                                const char *value, size_t value_len,
                                const char *ip, int port)
  {
    try
    {
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      async_simple::coro::syncAwait(
          datanodes_[node_ip_port]->call<&Datanode::handle_set>(
              ip_, port_for_transfer_data_, false));

      asio::error_code error;
      asio::ip::tcp::socket socket_(io_context_);
      asio::ip::tcp::resolver resolver(io_context_);
      asio::error_code con_error;
      asio::connect(socket_, resolver.resolve({std::string(ip), std::to_string(port + 500)}), con_error);
      if (!con_error && IF_DEBUG)
      {
        std::string msg = "[Socket] Connect to " + std::string(ip) + ":" + std::to_string(port + SOCKET_PORT_OFFSET) + " success!\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      std::vector<unsigned char> key_size_buf = int_to_bytes(key_len);
      asio::write(socket_, asio::buffer(key_size_buf, key_size_buf.size()));

      std::vector<unsigned char> value_size_buf = int_to_bytes(value_len);
      asio::write(socket_, asio::buffer(value_size_buf, value_size_buf.size()));

      asio::write(socket_, asio::buffer(key, key_len));
      asio::write(socket_, asio::buffer(value, value_len));

      std::vector<unsigned char> finish_buf(sizeof(int));
      asio::read(socket_, asio::buffer(finish_buf, finish_buf.size()));
      int finish = bytes_to_int(finish_buf);

      asio::error_code ignore_ec;
      socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket_.close(ignore_ec);

      if (!finish)
      {
        std::string errmsg = "[Set] Set " + std::string(key) + " failed!\n";
        write_logs(Logger::LogLevel::ERROR, errmsg);
      }
      else if (IF_DEBUG)
      {
        std::string msg = "[Set] Set " + std::string(key) + " success! With length of " + std::to_string(value_len) + " bytes\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
  }

  bool Proxy::read_from_datanode(const char *key, size_t key_len,
                                 char *value, size_t value_len,
                                 const char *ip, int port)
  {
    bool res = true;
    try
    {
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      async_simple::coro::syncAwait(
          datanodes_[node_ip_port]->call<&Datanode::handle_get>(
              std::string(key), key_len, value_len));
      if (IF_DEBUG)
      {
        std::string msg = "[Get] Call datanode to handle get " + std::string(key) + "\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      asio::error_code ec;
      asio::ip::tcp::socket socket_(io_context_);
      asio::ip::tcp::resolver resolver(io_context_);
      asio::error_code con_error;
      asio::connect(socket_, resolver.resolve({std::string(ip), std::to_string(port + SOCKET_PORT_OFFSET)}), con_error);

      std::vector<unsigned char> size_buf(sizeof(int));
      asio::read(socket_, asio::buffer(size_buf, size_buf.size()), ec);
      int key_size = bytes_to_int(size_buf);
      asio::read(socket_, asio::buffer(size_buf, size_buf.size()), ec);
      int value_size = bytes_to_int(size_buf);

      if (value_size > 0)
      {
        std::string key_buf(key_size, 0);
        asio::read(socket_, asio::buffer(key_buf.data(), key_buf.size()), ec);
        asio::read(socket_, asio::buffer(value, value_len), ec);

        std::vector<unsigned char> finish = int_to_bytes(1);
        asio::write(socket_, asio::buffer(finish, finish.size()));
        if (IF_DEBUG)
        {
          std::string msg = "[Get] Read data from socket with length of " + std::to_string(value_len) + " bytes\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }
      }
      else
      {
        std::string errmsg = "[Get] Get " + std::string(key) + " failed!\n";
        write_logs(Logger::LogLevel::ERROR, errmsg);
        res = false;
      }
      asio::error_code ignore_ec;
      socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket_.close(ignore_ec);
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
    return res;
  }

  void Proxy::delete_in_datanode(std::string block_id, const char *ip, int port)
  {
    try
    {
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      async_simple::coro::syncAwait(
          datanodes_[node_ip_port]->call<&Datanode::handle_delete>(block_id));
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
  }

  // source datanode -> destination datanode
  void Proxy::block_migration(const char *key, size_t key_len,
                              size_t value_len, const char *src_ip,
                              int src_port, const char *des_ip, int des_port)
  {
    try
    {
      std::string s_node_ip_port = std::string(src_ip) + ":" + std::to_string(src_port);
      async_simple::coro::syncAwait(
          datanodes_[s_node_ip_port]->call<&Datanode::handle_get>(
              std::string(key), key_len, value_len));
      if (IF_DEBUG)
      {
        std::string msg = "[Migration] Call datanode" + std::to_string(src_port) + " to handle get " + std::string(key) + "\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      std::string d_node_ip_port = std::string(des_ip) + ":" + std::to_string(des_port);
      async_simple::coro::syncAwait(
          datanodes_[d_node_ip_port]->call<&Datanode::handle_set>(
              src_ip, src_port + SOCKET_PORT_OFFSET, true));
      if (IF_DEBUG)
      {
        std::string msg = "[Migration] Call datanode" + std::to_string(des_port) + " to handle set " + std::string(key) + "\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
  }

  void Proxy::transfer_to_networkcore(const char *value, size_t value_len)
  {
    try
    {
      async_simple::coro::syncAwait(
          datanodes_[networkcore_]->call<&Datanode::handle_transfer>());

      std::string ip;
      int port;
      std::stringstream ss(networkcore_);
      std::getline(ss, ip, ':');
      ss >> port;

      asio::error_code error;
      asio::ip::tcp::socket socket_(io_context_);
      asio::ip::tcp::resolver resolver(io_context_);
      asio::error_code con_error;
      asio::connect(socket_, resolver.resolve({std::string(ip), std::to_string(port + SOCKET_PORT_OFFSET)}), con_error);
      if (!con_error && IF_DEBUG)
      {
        std::string msg = "[Socket] Connect to " + std::string(ip) + ":" + std::to_string(port + SOCKET_PORT_OFFSET) + " success!\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      std::vector<unsigned char> value_size_buf = int_to_bytes(value_len);
      asio::write(socket_, asio::buffer(value_size_buf, value_size_buf.size()));

      asio::write(socket_, asio::buffer(value, value_len));

      std::vector<unsigned char> finish_buf(sizeof(int));
      asio::read(socket_, asio::buffer(finish_buf, finish_buf.size()));

      asio::error_code ignore_ec;
      socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket_.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::string msg = "[Cross-cluster Transfer] Transfer success! With length of " + std::to_string(value_len) + " bytes\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
  }

  // non-blocked
  void Proxy::encode_and_store_object(PlacementInfo placement)
  {
    auto encode_and_store = [this, placement]() mutable
    {
      asio::ip::tcp::socket socket_(io_context_);
      acceptor_.accept(socket_);

      int stripe_num = (int)placement.stripe_ids.size();

      size_t value_buf_size = placement.value_len;

      std::vector<char> key_buf((int)placement.key.size(), 0);
      std::vector<char> value_buf(value_buf_size, 0);

      if (IF_DEBUG)
      {
        std::string msg = "[Set] Ready to receive value of " + placement.key + " with length of " + std::to_string(value_buf_size) + " bytes\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      std::vector<unsigned char> size_buf(sizeof(int));
      asio::read(socket_, asio::buffer(size_buf.data(), size_buf.size()));
      int key_size = bytes_to_int(size_buf);
      my_assert(key_size == (int)placement.key.size());

      asio::read(socket_, asio::buffer(size_buf.data(), size_buf.size()));
      int value_size = bytes_to_int(size_buf);
      my_assert(value_size == value_buf_size);

      size_t read_len_of_key = asio::read(socket_,
                                          asio::buffer(key_buf.data(), key_buf.size()));
      my_assert(read_len_of_key == key_buf.size());

      size_t read_len_of_value = asio::read(socket_,
                                            asio::buffer(value_buf.data(), value_buf_size));
      my_assert(read_len_of_value == value_buf_size);

      double encoding_time = 0;
      char *object_value = value_buf.data();
      if (placement.isreplica)
      {
        for (auto i = 0; i < placement.stripe_ids.size(); i++)
        {
          auto ec = ec_factory(placement.ec_type, placement.cp);
          std::vector<char *> data_v(ec->k);
          char **data = (char **)data_v.data();

          size_t cur_block_size = placement.block_size;
          my_assert(cur_block_size > 0);

          if (IF_DEBUG)
          {
            std::string msg = "[Set] Set value with size of " + std::to_string(ec->k * cur_block_size) + " bytes\n";
            write_logs(Logger::LogLevel::INFO, msg);
          }

          for (int j = 0; j < ec->k; j++)
          {
            data[j] = &object_value[j * cur_block_size];
          }

          int num_of_datanodes_involved = ec->k * (placement.from_two_replica ? 2 : 3);
          int num_of_blocks_each_stripe = num_of_datanodes_involved;

          if (IF_DEBUG)
          {
            std::string msg = "[Set] Distribute blocks to datanodes.\n";
            write_logs(Logger::LogLevel::INFO, msg);
          }

          int cross_cluster_num = 0;
          std::vector<std::thread> writers;
          int k = ec->k;
          for (int j = 0; j < num_of_datanodes_involved; j++)
          {
            std::string block_id = std::to_string(
                placement.block_ids[i * k + j % k]);
            unsigned int cluster_id =
                placement.datanode_ip_port[i * num_of_blocks_each_stripe + j].first;
            std::pair<std::string, int> ip_and_port_of_datanode =
                placement.datanode_ip_port[i * num_of_blocks_each_stripe + j].second;
            writers.push_back(
                std::thread([this, j, k, block_id, data, cur_block_size,
                             ip_and_port_of_datanode]()
                            { write_to_datanode(block_id.c_str(), block_id.size(),
                                                data[j % k], cur_block_size,
                                                ip_and_port_of_datanode.first.c_str(),
                                                ip_and_port_of_datanode.second); }));
            if (cluster_id != self_cluster_id_)
            { // to do
              cross_cluster_num++;
            }
          }
          for (auto j = 0; j < writers.size(); j++)
          {
            writers[j].join();
          }

          object_value += (ec->k * cur_block_size);

          if (cross_cluster_num && IF_SIMULATE_CROSS_CLUSTER_SET && IF_TEST_TRHROUGHPUT)
          {
            size_t t_val_len = (int)cur_block_size * cross_cluster_num;
            std::string t_value = generate_random_string((int)t_val_len);
            transfer_to_networkcore(t_value.c_str(), t_val_len);
          }
        }
      }
      else
      {
        for (auto i = 0; i < placement.stripe_ids.size(); i++)
        {
          auto ec = ec_factory(placement.ec_type, placement.cp);
          std::vector<char *> data_v(ec->k);
          std::vector<char *> coding_v(ec->m);
          char **data = (char **)data_v.data();
          char **coding = (char **)coding_v.data();

          size_t cur_block_size = placement.block_size;
          my_assert(cur_block_size > 0);

          if (IF_DEBUG)
          {
            std::string msg = "[Set] Encode value with size of " + std::to_string(ec->k * cur_block_size) + " bytes\n";
            write_logs(Logger::LogLevel::INFO, msg);
          }

          std::vector<std::vector<char>>
              space_for_parity_blocks(ec->m, std::vector<char>(cur_block_size));
          for (int j = 0; j < ec->k; j++)
          {
            data[j] = &object_value[j * cur_block_size];
          }
          for (int j = 0; j < ec->m; j++)
          {
            coding[j] = space_for_parity_blocks[j].data();
          }

          struct timeval start_time, end_time;
          gettimeofday(&start_time, NULL);
          ec->encode(data, coding, cur_block_size);
          gettimeofday(&end_time, NULL);
          encoding_time += end_time.tv_sec - start_time.tv_sec +
                           (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

          int num_of_datanodes_involved = ec->k + ec->m;
          int num_of_blocks_each_stripe = num_of_datanodes_involved;

          if (IF_DEBUG)
          {
            std::string msg = "[Set] Distribute blocks to datanodes.\n";
            write_logs(Logger::LogLevel::INFO, msg);
          }

          int cross_cluster_num = 0;
          std::vector<std::thread> writers;
          int k = ec->k;
          for (int j = 0; j < num_of_datanodes_involved; j++)
          {
            std::string block_id = std::to_string(
                placement.block_ids[i * num_of_blocks_each_stripe + j]);
            unsigned int cluster_id =
                placement.datanode_ip_port[i * num_of_blocks_each_stripe + j].first;
            std::pair<std::string, int> ip_and_port_of_datanode =
                placement.datanode_ip_port[i * num_of_blocks_each_stripe + j].second;
            writers.push_back(
                std::thread([this, j, k, block_id, data, coding, cur_block_size,
                             ip_and_port_of_datanode]()
                            {
                if (j < k) {
                  write_to_datanode(block_id.c_str(), block_id.size(), 
                                    data[j], cur_block_size,
                                    ip_and_port_of_datanode.first.c_str(),
                                    ip_and_port_of_datanode.second);
                  } else {
                  write_to_datanode(block_id.c_str(), block_id.size(),
                                    coding[j - k], cur_block_size,
                                    ip_and_port_of_datanode.first.c_str(),
                                    ip_and_port_of_datanode.second);
                  } }));
            if (cluster_id != self_cluster_id_)
            { // to do
              cross_cluster_num++;
            }
          }
          for (auto j = 0; j < writers.size(); j++)
          {
            writers[j].join();
          }

          object_value += (ec->k * cur_block_size);

          if (cross_cluster_num && IF_SIMULATE_CROSS_CLUSTER_SET && IF_TEST_TRHROUGHPUT)
          {
            size_t t_val_len = (int)cur_block_size * cross_cluster_num;
            std::string t_value = generate_random_string((int)t_val_len);
            transfer_to_networkcore(t_value.c_str(), t_val_len);
          }
        }
      }

      if (IF_DEBUG)
      {
        std::string msg = "[Set] Finish encode and set.\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      std::vector<unsigned char> finish = int_to_bytes(1);
      asio::write(socket_, asio::buffer(finish, finish.size()));

      std::vector<unsigned char> encoding_time_buf = double_to_bytes(encoding_time);
      asio::write(socket_, asio::buffer(encoding_time_buf, encoding_time_buf.size()));

      asio::error_code ignore_ec;
      socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket_.close(ignore_ec);
    };
    try
    {
      std::thread new_thread(encode_and_store);
      new_thread.detach();
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
  }

  // non-blocked
  void Proxy::decode_and_get_object(PlacementInfo placement)
  {
    auto decode_and_transfer = [this, placement]() mutable
    {     
      std::string object_value;
      auto ec = ec_factory(placement.ec_type, placement.cp);
      ec->init_coding_parameters(placement.cp);
      int stripe_num = (int)placement.stripe_ids.size();
      int left_value_len = (int)placement.value_len;
      unsigned int stripe_id = placement.stripe_ids[0];
      auto blocks_ptr = std::make_shared<std::unordered_map<int, std::string>>();
      auto block_idxs_ptr = std::make_shared<std::unordered_map<int, int>>();

      size_t cur_block_size = placement.block_size;
      my_assert(cur_block_size > 0);

      if (IF_DEBUG)
      {
        std::string msg = "[GET] Ready to get blocks from datanodes. The block size is " + std::to_string(cur_block_size) + " bytes\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      // read the k data blocks
      int num_of_datanodes_involved = ec->k;
      int offset = placement.offset;
      left_value_len -= (ec->k - offset) * cur_block_size;
      if (left_value_len < 0)
      {
        left_value_len += (ec->k - offset) * cur_block_size;
        num_of_datanodes_involved = std::ceil(static_cast<double>(left_value_len) /
                                              static_cast<double>(cur_block_size));
      }
      else
      {
        num_of_datanodes_involved = ec->k - offset;
      }
      int num_of_blocks_each_stripe = ec->k + ec->m;
      int cross_cluster_num = 0;
      auto ec_family = check_ec_family(placement.ec_type);
      bool flag = placement.merged_flag;
      flag = (flag && ec_family == PCs && !placement.isvertical);
      int cnt1 = 0, cnt2 = 0;
      
      // =================================================================
      int read_mode = placement.read_mode;  // 
      int target_trigger_idx = placement.offset; 
      
      std::vector<int> simulated_dead_blocks;

      if (read_mode == 1) {
          simulated_dead_blocks.push_back(target_trigger_idx);
      }
      else if (read_mode == 3) {
          unsigned int target_cluster_id = placement.datanode_ip_port[target_trigger_idx].first;
          for (int i = 0; i < ec->k + ec->m; i++) {
              if (placement.datanode_ip_port[i].first == target_cluster_id) {
                  simulated_dead_blocks.push_back(i);
              }
          }
      } 
      else if (read_mode == 4) {
          int target_port = placement.datanode_ip_port[target_trigger_idx].second.second; 
          for (int i = 0; i < ec->k + ec->m; i++) {
              int current_port = placement.datanode_ip_port[i].second.second;
              if (current_port % 10 == target_port % 10) { 
                  simulated_dead_blocks.push_back(i);
              }
          }
      }

      std::vector<std::thread> readers;
      for (int j = 0; j < num_of_datanodes_involved; j++)
      {
        int idx = j + offset;

        if (std::find(simulated_dead_blocks.begin(), simulated_dead_blocks.end(), idx) != simulated_dead_blocks.end()) {
            if (IF_DEBUG) {
                std::string msg = "[Get] Inject simulated failure for block " + std::to_string(idx) + "\n";
                write_logs(Logger::LogLevel::INFO, msg);
            }
            continue; 
        }

        if (flag)
        {
          idx -= offset;
          idx += placement.cp.seri_num * placement.cp.k1 / placement.cp.x + cnt2 * placement.cp.k1 * (placement.cp.x - 1) / placement.cp.x;
          cnt1++;
          if (cnt1 == placement.cp.k1 / placement.cp.x)
          {
            cnt1 = 0;
            cnt2++;
          }
        }
        unsigned int cluster_id =
            placement.datanode_ip_port[idx].first;
        std::pair<std::string, int> ip_and_port_of_datanode =
            placement.datanode_ip_port[idx].second;
        readers.push_back(
            std::thread([this, idx, j, blocks_ptr, block_idxs_ptr, cur_block_size,
                         ip_and_port_of_datanode, placement]()
                        {
            std::string block_id =
                std::to_string(placement.block_ids[idx]);

            std::string block(cur_block_size, 0);
            auto res = read_from_datanode(block_id.c_str(), block_id.size(),
                                          block.data(), cur_block_size,
                                          ip_and_port_of_datanode.first.c_str(),
                                          ip_and_port_of_datanode.second);
            if (!res) {
              //pthread_exit(NULL);
              //std::cerr << "reader thread failed" << std::endl;
              std::cerr << "reader thread failed for block " << idx << std::endl;
              return;
            }
            mutex_.lock();
            (*blocks_ptr)[j] = block;
            (*block_idxs_ptr)[j] = idx;
            mutex_.unlock(); }));
        if (cluster_id != self_cluster_id_)
        { // to do
          cross_cluster_num++;
        }
      }
      for (auto j = 0; j < readers.size(); j++)
      {
        readers[j].join();
      }

      // for degraded read
      if (blocks_ptr->size() < num_of_datanodes_involved)
      {
        if (IF_DEBUG)
        {
          std::string msg = "[Get] Encounter degraded read!\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }
        auto retrieved_idx = std::vector<int>();
        auto failures_idx = std::vector<int>();
        auto idxinres = std::vector<int>();
        for (int j = 0; j < num_of_datanodes_involved; j++)
        {
          int idx = j + offset;
          if (block_idxs_ptr->find(idx) == block_idxs_ptr->end())
          {
            failures_idx.push_back(idx);
            idxinres.push_back(j);
          }
          else
          {
            retrieved_idx.push_back(idx);
          }
        }

        for (int dead_idx : simulated_dead_blocks) {
            if (std::find(failures_idx.begin(), failures_idx.end(), dead_idx) == failures_idx.end()) {
                failures_idx.push_back(dead_idx);
            }
        }

        if (IF_DEBUG)
        {
          std::string msg = "[Get] Unreachable blocks: ";
          for (auto idx : failures_idx)
          {
            msg += std::to_string(idx) + " ";
          }
          msg += "\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }

        if (placement.isreplica)
        { // recover from the second replica
          std::vector<std::thread> readers;
          cnt1 = 0;
          cnt2 = 0;
          int i = 0;
          for (auto idx : failures_idx)
          {
            idx = ec->k + idx;
            if (flag)
            {
              idx -= offset;
              idx += placement.cp.seri_num * placement.cp.k1 / placement.cp.x + cnt2 * placement.cp.k1 * (placement.cp.x - 1) / placement.cp.x;
              cnt1++;
              if (cnt1 == placement.cp.k1 / placement.cp.x)
              {
                cnt1 = 0;
                cnt2++;
              }
            }
            unsigned int cluster_id =
                placement.datanode_ip_port[idx].first;
            std::pair<std::string, int> ip_and_port_of_datanode =
                placement.datanode_ip_port[idx].second;
            int j = idxinres[i++];
            readers.push_back(
                std::thread([this, idx, j, blocks_ptr, block_idxs_ptr, cur_block_size,
                             ip_and_port_of_datanode, placement]()
                            {
                std::string block_id =
                    std::to_string(placement.block_ids[idx]);

                std::string block(cur_block_size, 0);
                auto res = read_from_datanode(block_id.c_str(), block_id.size(),
                                              block.data(), cur_block_size,
                                              ip_and_port_of_datanode.first.c_str(),
                                              ip_and_port_of_datanode.second);
                if (!res) {
                  //pthread_exit(NULL);
                  std::cerr << "reader thread failed" << std::endl;
                  return;
                }
                mutex_.lock();
                (*blocks_ptr)[j] = block;
                (*block_idxs_ptr)[j] = idx;
                mutex_.unlock(); }));
            if (cluster_id != self_cluster_id_)
            { // to do
              cross_cluster_num++;
            }
          }
          for (auto j = 0; j < readers.size(); j++)
          {
            readers[j].join();
          }
          for (int j = 0; j < num_of_datanodes_involved; j++)
          {
            object_value += (*blocks_ptr)[j];
          }
        }
        else
        {
          auto help_blocks_index = std::vector<int>();
          // find out placement
          ec->partition_plan.clear();
          std::unordered_map<unsigned int, std::vector<int>> blocks_in_clusters;
          for (int i = 0; i < ec->k + ec->m; i++)
          {
            unsigned int cluster_id = placement.datanode_ip_port[i].first;
            if (blocks_in_clusters.find(cluster_id) == blocks_in_clusters.end())
            {
              blocks_in_clusters[cluster_id] = std::vector<int>({i});
            }
            else
            {
              blocks_in_clusters[cluster_id].push_back(i);
            }
          }
          for (auto &kv : blocks_in_clusters)
          {
            ec->partition_plan.push_back(kv.second);
          }
          if (IF_DEBUG)
          {
            ec->print_info(ec->partition_plan, "placement");
          }

          std::vector<RepairPlan> all_repair_plans;
          ec->generate_repair_plan(failures_idx, all_repair_plans, true, true, false);
          ec->local_or_column = false;

          std::unordered_set<int> required_blocks;
          required_blocks.insert(target_trigger_idx); 

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
                              if (std::find(simulated_dead_blocks.begin(), simulated_dead_blocks.end(), h_idx) != simulated_dead_blocks.end()) {
                                  required_blocks.insert(h_idx); 
                              }
                          }
                      }
                  }
              }
          }

          auto toretrieve_block_idxs = std::vector<int>();
          for (size_t i = 0; i < all_repair_plans.size(); i++) {
              if (plan_included[i]) {
                  for (auto &help_group : all_repair_plans[i].help_blocks) {
                      for (int h_idx : help_group) {
                          if (std::find(retrieved_idx.begin(), retrieved_idx.end(), h_idx) == retrieved_idx.end() &&
                              std::find(simulated_dead_blocks.begin(), simulated_dead_blocks.end(), h_idx) == simulated_dead_blocks.end() &&
                              std::find(toretrieve_block_idxs.begin(), toretrieve_block_idxs.end(), h_idx) == toretrieve_block_idxs.end()) 
                          {
                              toretrieve_block_idxs.push_back(h_idx);
                          }
                      }
                  }
              }
          }

          int num_of_blocks_to_retrieve = toretrieve_block_idxs.size();
          auto toretrieve_blocks_ptr = std::make_shared<std::unordered_map<int, std::string>>();
          auto toretrieve_block_idxs_ptr = std::make_shared<std::unordered_map<int, int>>();
          std::vector<std::thread> retrievers;
          for (int j = 0; j < num_of_blocks_to_retrieve; j++)
          {
            unsigned int cluster_id =
                placement.datanode_ip_port[toretrieve_block_idxs[j]].first;
            std::pair<std::string, int> ip_and_port_of_datanode =
                placement.datanode_ip_port[toretrieve_block_idxs[j]].second;
            if (cluster_id != self_cluster_id_) {
                cross_cluster_num++; 
            }
            retrievers.push_back(
                std::thread([this, j, toretrieve_block_idxs, toretrieve_blocks_ptr,
                             toretrieve_block_idxs_ptr, cur_block_size, placement,
                             ip_and_port_of_datanode]()
                            {
                std::string block_id =
                    std::to_string(placement.block_ids[toretrieve_block_idxs[j]]);

                std::string block(cur_block_size, 0);
                auto res = read_from_datanode(block_id.c_str(), block_id.size(),
                                              block.data(), cur_block_size,
                                              ip_and_port_of_datanode.first.c_str(),
                                              ip_and_port_of_datanode.second);
                if (!res) {
                 // pthread_exit(NULL);
                 return;
                }
                mutex_.lock();
                (*toretrieve_blocks_ptr)[j] = block;
                (*toretrieve_block_idxs_ptr)[j] = toretrieve_block_idxs[j];
                mutex_.unlock(); }));
          }
          for (auto j = 0; j < retrievers.size(); j++)
          {
            retrievers[j].join();
          }
         // my_assert(toretrieve_blocks_ptr->size() == num_of_blocks_to_retrieve);
          if (toretrieve_blocks_ptr->size() < num_of_blocks_to_retrieve) {
              std::string msg = "[Error] Failed to retrieve enough help blocks! System exceeded tolerance.\n";
              write_logs(Logger::LogLevel::ERROR, msg);
              
              return; 
          }
          
          if (IF_DEBUG)
          {
            std::string msg = "[Get] Retrieve more blocks: ";
            for (auto idx : toretrieve_block_idxs)
            {
              msg += std::to_string(idx) + " ";
            }
            msg += "\n";
            write_logs(Logger::LogLevel::INFO, msg);
          }

          std::vector<char *> v_data(ec->k);
          std::vector<char *> v_coding(ec->m);
          char **data = (char **)v_data.data();
          char **coding = (char **)v_coding.data();
          std::vector<std::vector<char>>
              v_data_area(ec->k, std::vector<char>(cur_block_size));
          std::vector<std::vector<char>>
              v_coding_area(ec->m, std::vector<char>(cur_block_size));
          for (int j = 0; j < ec->k; j++)
          {
            data[j] = v_data_area[j].data();
          }
          for (int j = 0; j < ec->m; j++)
          {
            coding[j] = v_coding_area[j].data();
          }

          for (auto &kv : *blocks_ptr)
          {
            int idx = (*block_idxs_ptr)[kv.first];
            if (idx < ec->k)
            {
              data[idx] = kv.second.data();
            }
            else
            {
              coding[idx - ec->k] = kv.second.data();
            }
          }
          for (auto &kv : *toretrieve_blocks_ptr)
          {
            int idx = (*toretrieve_block_idxs_ptr)[kv.first];
            if (idx < ec->k)
            {
              data[idx] = kv.second.data();
            }
            else
            {
              coding[idx - ec->k] = kv.second.data();
            }
          }

          int failed_num = (int)failures_idx.size();
          int erasures[failed_num + 1];
          for (int j = 0; j < failed_num; j++)
          {
            erasures[j] = failures_idx[j];
          }
          erasures[failed_num] = -1;

          if (IF_DEBUG)
          {
            std::string msg = "[Get] ready to decode! " + ec->self_information() + "\n";
            write_logs(Logger::LogLevel::INFO, msg);
          }

          ec->decode(data, coding, cur_block_size, erasures, failed_num);

          if (IF_DEBUG)
          {
            std::string msg = "[Get] reconstruct unreachable blocks successfully!\n";
            write_logs(Logger::LogLevel::INFO, msg);
          }

          for (int j = 0; j < num_of_datanodes_involved; j++)
          {
            int idx = j + offset;
            object_value += std::string(data[idx], cur_block_size);
          }
        }
      }
      else
      {
        for (int j = 0; j < num_of_datanodes_involved; j++)
        {
          object_value += (*blocks_ptr)[j];
        }
      }

      if (cross_cluster_num && IF_SIMULATE_CROSS_CLUSTER_GET_REPAIR && IF_TEST_TRHROUGHPUT)
      {
        size_t t_val_len = (int)cur_block_size * cross_cluster_num;
        std::string t_value = generate_random_string((int)t_val_len);
        transfer_to_networkcore(t_value.c_str(), t_val_len);
      }

      asio::ip::tcp::socket socket_(io_context_);
      asio::ip::tcp::endpoint endpoint(
          asio::ip::make_address(placement.client_ip), placement.client_port);
      socket_.connect(endpoint);

      std::vector<unsigned char> key_size_buf = int_to_bytes(placement.key.size());
      asio::write(socket_, asio::buffer(key_size_buf, key_size_buf.size()));

      std::vector<unsigned char> value_size_buf = int_to_bytes(object_value.size());
      asio::write(socket_, asio::buffer(value_size_buf, value_size_buf.size()));

      asio::write(socket_, asio::buffer(placement.key, placement.key.size()));
      asio::write(socket_, asio::buffer(object_value, object_value.size()));

      if (IF_DEBUG)
      {
        std::string msg = "[Get] get " + placement.key + " successfully!\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      asio::error_code ignore_ec;
      socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket_.close(ignore_ec);
    };
    try
    {
      std::thread new_thread(decode_and_transfer);
      new_thread.detach();
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
  }


  void Proxy::delete_blocks(DeletePlan delete_info)
  {
    auto delete_blocks_in_stripe = [this, delete_info]()
    {
      my_assert(delete_info.block_ids.size() ==
                delete_info.blocks_info.size());
      int num_of_blocks_to_delete = delete_info.block_ids.size();
      std::vector<std::thread> deleters;
      for (int i = 0; i < num_of_blocks_to_delete; i++)
      {
        std::pair<std::string, int> ip_and_port_of_datanode =
            delete_info.blocks_info[i];
        std::string block_id = std::to_string(delete_info.block_ids[i]);
        deleters.push_back(
            std::thread([this, block_id, ip_and_port_of_datanode]()
                        { delete_in_datanode(block_id, ip_and_port_of_datanode.first.c_str(),
                                             ip_and_port_of_datanode.second); }));
      }
      for (int i = 0; i < num_of_blocks_to_delete; i++)
      {
        deleters[i].join();
      }
    };
    try
    {
      std::thread new_thread(delete_blocks_in_stripe);
      new_thread.join();
      if (IF_DEBUG)
      {
        std::string msg = "[Del] delete blocks successfully!\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
  }

  RelocateResp Proxy::block_relocation(RelocatePlan reloc_plan)
  {
    auto migrate_a_block = [this, reloc_plan](int i) mutable
    {
      std::string block_id = std::to_string(reloc_plan.blocks_to_move[i]);
      block_migration(block_id.c_str(),
                      block_id.size(),
                      reloc_plan.block_size,
                      reloc_plan.src_nodes[i].second.first.c_str(),
                      reloc_plan.src_nodes[i].second.second,
                      reloc_plan.des_nodes[i].second.first.c_str(),
                      reloc_plan.des_nodes[i].second.second);
      delete_in_datanode(std::to_string(reloc_plan.blocks_to_move[i]).c_str(),
                         reloc_plan.src_nodes[i].second.first.c_str(),
                         reloc_plan.src_nodes[i].second.second);
    };
    RelocateResp reloc_resp;
    try
    {
      int cross_cluster_num = 0;
      std::vector<std::thread> migrators;
      int num_of_blocks = int(reloc_plan.blocks_to_move.size());
      for (int i = 0; i < num_of_blocks; i++)
      {
        migrators.push_back(std::thread(migrate_a_block, i));
        if (reloc_plan.src_nodes[i].first != reloc_plan.des_nodes[i].first)
        { // to do
          cross_cluster_num++;
        }
      }
      for (int i = 0; i < num_of_blocks; i++)
      {
        migrators[i].join();
      }
      struct timeval start_time, end_time;
      gettimeofday(&start_time, NULL);
      if (IF_SIMULATE_CROSS_CLUSTER_GET_REPAIR)
      {
        size_t t_val_len = (int)reloc_plan.block_size * cross_cluster_num;
        std::string t_value = generate_random_string((int)t_val_len);
        transfer_to_networkcore(t_value.c_str(), t_val_len);
      }
      gettimeofday(&end_time, NULL);
      reloc_resp.cross_cluster_time = end_time.tv_sec - start_time.tv_sec +
                                      (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
    return reloc_resp;
  }

  EncodeResp Proxy::encode_stripe(EncodePlan encode_plan)
  {
    struct timeval start_time, end_time;
    double encoding_time = 0;
    double cross_cluster_time = 0;

    bool partial_encoding = encode_plan.partial_encoding;
    size_t block_size = encode_plan.block_size;
    auto ec = ec_factory(encode_plan.ec_type, encode_plan.cp);
    ec->init_coding_parameters(encode_plan.cp);

    int parity_num = encode_plan.new_parity_block_ids.size();
    auto parity_idx_ptr = std::make_shared<std::vector<int>>();
    for (int i = 0; i < parity_num; i++)
    {
      parity_idx_ptr->push_back(encode_plan.new_locations[i].first);
    }

    auto original_lock_ptr = std::make_shared<std::mutex>();
    auto original_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
    auto original_blocks_idx_ptr = std::make_shared<std::vector<int>>();

    auto get_from_node = [this, original_lock_ptr, original_blocks_ptr,
                          original_blocks_idx_ptr, block_size](
                             int block_idx, unsigned int block_id,
                             std::string node_ip, int node_port) mutable
    {
      std::string block_id_str = std::to_string(block_id);
      std::vector<char> tmp_val(block_size);
      bool res = read_from_datanode(block_id_str.c_str(), block_id_str.size(),
                                    tmp_val.data(), block_size, node_ip.c_str(), node_port);
      if (!res)
      {
        pthread_exit(NULL);
      }
      original_lock_ptr->lock();
      original_blocks_ptr->push_back(tmp_val);
      original_blocks_idx_ptr->push_back(block_idx);
      original_lock_ptr->unlock();
    };

    auto send_to_datanode = [this, block_size](
                                unsigned int block_id, char *data, std::string node_ip, int node_port)
    {
      std::string block_id_str = std::to_string(block_id);
      write_to_datanode(block_id_str.c_str(), block_id_str.size(),
                        data, block_size, node_ip.c_str(), node_port);
    };

    // get data blocks inside cluster
    int num_of_data_blocks =
        (int)encode_plan.data_blocks_info.size();
    if (num_of_data_blocks > 0)
    {
      std::vector<std::thread> readers;
      for (int i = 0; i < num_of_data_blocks; i++)
      {
        readers.push_back(std::thread(get_from_node,
                                      encode_plan.data_blocks_info[i].first,
                                      encode_plan.data_block_ids[i],
                                      encode_plan.data_blocks_info[i].second.first,
                                      encode_plan.data_blocks_info[i].second.second));
      }
      for (int i = 0; i < num_of_data_blocks; i++)
      {
        readers[i].join();
      }
    }
    if (IF_DEBUG)
    {
      std::string msg = "Finish getting " + std::to_string(num_of_data_blocks) + " data blocks inside cluster.\n";
      write_logs(Logger::LogLevel::INFO, msg);
    }

    // encode
    if (encode_plan.partial_encoding)
    {
      std::vector<char *> v_data(num_of_data_blocks);
      std::vector<char *> v_coding(parity_num);
      char **data = (char **)v_data.data();
      char **coding = (char **)v_coding.data();
      std::vector<std::vector<char>> v_coding_area(parity_num, std::vector<char>(block_size));
      for (int j = 0; j < parity_num; j++)
      {
        coding[j] = v_coding_area[j].data();
      }
      for (int j = 0; j < num_of_data_blocks; j++)
      {
        data[j] = (*original_blocks_ptr)[j].data();
      }
      auto nothing_ptr = std::make_shared<std::vector<int>>();
      auto partial_flags = std::vector<bool>(parity_num, true);
      gettimeofday(&start_time, NULL);
      ec->encode_partial_blocks(data, coding, block_size,
                                *original_blocks_idx_ptr, *parity_idx_ptr, *nothing_ptr,
                                *nothing_ptr, partial_flags, false);
      gettimeofday(&end_time, NULL);
      encoding_time = end_time.tv_sec - start_time.tv_sec +
                      (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

      if (IF_DEBUG)
      {
        std::string msg = "Finish encoding, and then distribute parity blocks!\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      std::vector<std::thread> writers;
      for (int i = 0; i < parity_num; i++)
      {
        // std::cout << encode_plan.new_parity_block_ids[i] << " to " << encode_plan.new_locations[i].second.second << std::endl;
        writers.push_back(std::thread(send_to_datanode,
                                      encode_plan.new_parity_block_ids[i], v_coding[i],
                                      encode_plan.new_locations[i].second.first,
                                      encode_plan.new_locations[i].second.second));
      }
      for (int i = 0; i < parity_num; i++)
      {
        writers[i].join();
      }
    }
    else
    {
      my_assert(num_of_data_blocks == ec->k);
      std::vector<char *> v_data(ec->k);
      std::vector<char *> v_coding(ec->m);
      char **data = (char **)v_data.data();
      char **coding = (char **)v_coding.data();

      std::vector<std::vector<char>>
          space_for_parity_blocks(ec->m, std::vector<char>(block_size));
      for (int j = 0; j < ec->k; j++)
      {
        int idx = (*original_blocks_idx_ptr)[j];
        my_assert(idx < ec->k);
        data[idx] = (*original_blocks_ptr)[j].data();
      }
      for (int j = 0; j < ec->m; j++)
      {
        coding[j] = space_for_parity_blocks[j].data();
      }

      struct timeval start_time, end_time;
      gettimeofday(&start_time, NULL);
      ec->encode(data, coding, block_size);
      gettimeofday(&end_time, NULL);
      encoding_time += end_time.tv_sec - start_time.tv_sec +
                       (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

      if (IF_DEBUG)
      {
        std::string msg = "Finish encoding, and then distribute parity blocks!\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      std::vector<std::thread> writers;
      for (int i = 0; i < parity_num; i++)
      {
        int idx = encode_plan.new_locations[i].first;
        // std::cout << encode_plan.new_parity_block_ids[i] << "|" << idx << " to " << encode_plan.new_locations[i].second.second << std::endl;
        writers.push_back(std::thread(send_to_datanode,
                                      encode_plan.new_parity_block_ids[i], v_coding[idx - ec->k],
                                      encode_plan.new_locations[i].second.first,
                                      encode_plan.new_locations[i].second.second));
      }
      for (int i = 0; i < parity_num; i++)
      {
        writers[i].join();
      }
    }

    gettimeofday(&start_time, NULL);
    if (IF_SIMULATE_CROSS_CLUSTER_GET_REPAIR)
    {
      size_t t_val_len = (int)block_size * encode_plan.cross_cluster_num;
      std::string t_value = generate_random_string((int)t_val_len);
      transfer_to_networkcore(t_value.c_str(), t_val_len);
    }
    gettimeofday(&end_time, NULL);
    cross_cluster_time += end_time.tv_sec - start_time.tv_sec +
                          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

    if (IF_DEBUG)
    {
      std::string msg = "Finish encoding " + std::to_string(parity_num) + " blocks! Encdoing time : " + std::to_string(encoding_time) + "\n";
      write_logs(Logger::LogLevel::INFO, msg);
    }

    if (ec != nullptr)
    {
      delete ec;
      ec = nullptr;
    }

    EncodeResp encode_resp;
    encode_resp.encoding_time = encoding_time;
    encode_resp.cross_cluster_time = cross_cluster_time;
    return encode_resp;
  }
}
