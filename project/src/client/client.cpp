#include "client.h"
#include <unistd.h>

namespace ECProject
{
  Client::Client(std::string ip, int port, std::string coordinator_ip,
                 int coordinator_port)
    : ip_(ip), port_(port), coordinator_ip_(coordinator_ip),
      coordinator_port_(coordinator_port),
      acceptor_(io_context_,
        asio::ip::tcp::endpoint(asio::ip::address::from_string(ip.c_str()), port_)) 
  {
    easylog::set_min_severity(easylog::Severity::ERROR);
    rpc_coordinator_ = std::make_unique<coro_rpc::coro_rpc_client>();
    async_simple::coro::syncAwait(
        rpc_coordinator_->connect(coordinator_ip_, std::to_string(coordinator_port_)));
  }

  Client::~Client() { acceptor_.close(); }

  void Client::set_ec_parameters(ParametersInfo parameters)
  {
    async_simple::coro::syncAwait(
        rpc_coordinator_->call<&Coordinator::set_erasure_coding_parameters>(parameters));
  }

  double Client::set(const std::string& value,
                     const std::vector<std::string>& object_keys,
                     const std::vector<size_t>& object_sizes,
                     const std::vector<unsigned int>& object_accessrates)
  {
    auto response =
        async_simple::coro::syncAwait(
            rpc_coordinator_->call<&Coordinator::request_set>(
                object_keys, object_sizes, object_accessrates)).value();
        
    std::cout << "[SET] Send objects to proxy_address:" 
              << response.proxy_ip << ":" << response.proxy_port 
              << std::endl;

    double encoding_time = 0;
    if (!IF_SIMULATION) {
      asio::ip::tcp::socket socket_(io_context_);
      asio::ip::tcp::endpoint endpoint(asio::ip::make_address(response.proxy_ip),
                                       response.proxy_port);
      socket_.connect(endpoint);

      std::vector<unsigned char> key_size_buf = int_to_bytes((int)object_keys[0].size());
      asio::write(socket_, asio::buffer(key_size_buf, key_size_buf.size()));
      std::vector<unsigned char> value_size_buf = int_to_bytes((int)value.size());
        asio::write(socket_, asio::buffer(value_size_buf, value_size_buf.size()));

      asio::write(socket_, asio::buffer(object_keys[0], object_keys[0].size()));
      asio::write(socket_, asio::buffer(value, value.size()));

      std::vector<unsigned char> finish_buf(sizeof(int));
      asio::read(socket_, asio::buffer(finish_buf,
                                       finish_buf.size()));
      int finish = bytes_to_int(finish_buf);

      std::vector<unsigned char> encoding_time_buf(sizeof(double));
      asio::read(socket_, asio::buffer(encoding_time_buf,
                                       encoding_time_buf.size()));
      encoding_time = bytes_to_double(encoding_time_buf);

      asio::error_code ignore_ec;
      socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket_.close(ignore_ec);

      async_simple::coro::syncAwait(
          rpc_coordinator_->call<&Coordinator::commit_object>(object_keys, (bool)finish));
    }

    return encoding_time;
  }

  std::string Client::get(std::string key)
  {
    size_t value_len = 
        async_simple::coro::syncAwait(
            rpc_coordinator_->call<&Coordinator::request_get>(key, ip_, port_)).value();

    std::string key_buf(key.size(), 0);
    std::string value_buf(value_len, 0);

    if (!IF_SIMULATION) {
      asio::ip::tcp::socket socket_(io_context_);
      acceptor_.accept(socket_);

      std::vector<unsigned char> size_buf(sizeof(int));
      asio::read(socket_, asio::buffer(size_buf, size_buf.size()));
      int key_size = bytes_to_int(size_buf);
      asio::read(socket_, asio::buffer(size_buf, size_buf.size()));
      int value_size = bytes_to_int(size_buf);
      if (value_size > 0) {
        size_t read_len_of_key = 
            asio::read(socket_, asio::buffer(key_buf.data(), key_buf.size()));
        my_assert(read_len_of_key == key.size() && key_buf == key);

        size_t read_len_of_value =
            asio::read(socket_, asio::buffer(value_buf.data(), value_buf.size()));
        my_assert(read_len_of_value == value_len);

        asio::error_code ignore_ec;
        socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
        socket_.close(ignore_ec);

        //std::cout << "[GET] get key: " << key_buf.data()
        //          << ", valuesize: " << value_len << std::endl;
      } else {
        std::cerr << "[GET] can not get value of " << key_buf.data() << std::endl;
      }
    }

    return value_buf;
  }

  void Client::delete_stripe(unsigned int stripe_id)
  {
    std::vector<unsigned int> stripe_ids;
    stripe_ids.push_back(stripe_id);
    async_simple::coro::syncAwait(
        rpc_coordinator_->call<&Coordinator::request_delete_by_stripe>(stripe_ids));
    std::cout << "[DEL] deleting Stripe " << stripe_id << std::endl;
  }

  void Client::delete_all_stripes()
  {
    auto stripe_ids =
        async_simple::coro::syncAwait(
            rpc_coordinator_->call<&Coordinator::list_stripes>()).value();
    async_simple::coro::syncAwait(
        rpc_coordinator_->call<&Coordinator::request_delete_by_stripe>(stripe_ids));
    for (auto it = stripe_ids.begin(); it != stripe_ids.end(); it++) {
      std::cout << "[DEL] deleting Stripe " << *it << std::endl;
    }
  }

  RepairResp Client::nodes_repair(std::vector<unsigned int> failed_node_ids)
  {
    auto response = 
        async_simple::coro::syncAwait(
            rpc_coordinator_->call_for<&Coordinator::request_repair>(
                std::chrono::seconds{1200}, failed_node_ids, -1, 0)).value();
    return response;
  }

  RepairResp Client::blocks_repair(std::vector<unsigned int> failed_block_ids, int stripe_id, int maintenance_type)
  {
    auto response = 
        async_simple::coro::syncAwait(
            rpc_coordinator_->call_for<&Coordinator::request_repair>(
                std::chrono::seconds{1200}, failed_block_ids, stripe_id, maintenance_type)).value();
    return response;
  }

  MergeResp Client::merge(int step_size)
  {
    auto response =
        async_simple::coro::syncAwait(
            rpc_coordinator_->call_for<&Coordinator::request_merge>(
                std::chrono::seconds{1200}, step_size)).value();
    return response;
  }

  /* for Non-uniform LRC */
  ScaleResp Client::scale(float storage_overhead_upper, float gamma) 
  {
    auto response =
        async_simple::coro::syncAwait(
            rpc_coordinator_->call_for<&Coordinator::request_scale>(
                std::chrono::seconds{1200}, storage_overhead_upper, gamma)).value();
    return response;
  }

  TransResp Client::redundancy_transition(bool optimized)
  {
    auto response =
        async_simple::coro::syncAwait(
            rpc_coordinator_->call_for<&Coordinator::request_redundancy_transition>(
                std::chrono::seconds{1200}, optimized)).value();
    return response;
  }

  std::vector<unsigned int> Client::list_stripes()
  {
    auto response =
        async_simple::coro::syncAwait(
            rpc_coordinator_->call<&Coordinator::list_stripes>()).value();
    return response;
  }
}
