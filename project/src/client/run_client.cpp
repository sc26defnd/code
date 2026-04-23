#include "client.h"
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <regex>
#include <chrono>
#include <thread>
#include <atomic>

using namespace ECProject;

void test_single_block_repair(Client &client, int block_num)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<double> meta_times;
  std::vector<int> cross_cluster_transfers;
  std::vector<int> io_cnts;
  std::cout << "Single-Block Repair:" << std::endl;
  for (int i = 0; i < stripe_num; i++)
  {
    std::cout << "[Stripe " << i << "]" << std::endl;
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    double temp_meta = 0;
    int temp_cc_transfers = 0;
    int temp_io_cnt = 0;
    for (int j = 0; j < block_num; j++)
    {
      std::vector<unsigned int> failures;
      failures.push_back((unsigned int)j);
      auto resp = client.blocks_repair(failures, stripe_ids[i]);
      temp_repair += resp.repair_time;
      temp_decoding += resp.decoding_time;
      temp_cross_cluster += resp.cross_cluster_time;
      temp_meta += resp.meta_time;
      temp_cc_transfers += resp.cross_cluster_transfers;
      temp_io_cnt += resp.io_cnt;
    }
    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    meta_times.push_back(temp_meta);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    io_cnts.push_back(temp_io_cnt);
    std::cout << "repair = " << temp_repair / block_num
              << "s, decoding = " << temp_decoding / block_num
              << "s, cross-cluster = " << temp_cross_cluster / block_num
              << "s, meta = " << temp_meta / block_num
              << "s, cross-cluster-count = " << (double)temp_cc_transfers / block_num
              << ", I/Os = " << temp_io_cnt / block_num
              << std::endl;
  }
  auto avg_repair = std::accumulate(repair_times.begin(),
                                    repair_times.end(), 0.0) /
                    (stripe_num * block_num);
  auto avg_decoding = std::accumulate(decoding_times.begin(),
                                      decoding_times.end(), 0.0) /
                      (stripe_num * block_num);
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(),
                                           cross_cluster_times.end(), 0.0) /
                           (stripe_num * block_num);
  auto avg_meta = std::accumulate(meta_times.begin(),
                                  meta_times.end(), 0.0) /
                  (stripe_num * block_num);
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(),
                                                  cross_cluster_transfers.end(), 0) /
                          (stripe_num * block_num);
  auto avg_io_cnt = (double)std::accumulate(io_cnts.begin(),
                                            io_cnts.end(), 0) /
                    (stripe_num * block_num);
  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster = " << avg_cross_cluster
            << "s, meta = " << avg_meta
            << "s, cross-cluster-count = " << avg_cc_transfers
            << ", I/Os = " << avg_io_cnt
            << std::endl;
}


// (maintenance_type: 3=Rack, 4=Zone)
void test_maintenance_repair(Client &client, int block_num, int maintenance_type)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<int> cross_cluster_transfers;
  std::vector<int> io_cnts;

  std::string m_name = (maintenance_type == 3) ? "Rack" : "Zone";

  for (int i = 0; i < stripe_num; i++)
  {
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    int temp_cc_transfers = 0;
    int temp_io_cnt = 0;

    for (int j = 0; j < block_num; j++)
    {
      std::vector<unsigned int> target_failures;
      target_failures.push_back((unsigned int)j); 

      auto resp = client.blocks_repair(target_failures, stripe_ids[i], maintenance_type);

      if (resp.success) {
        temp_repair += resp.repair_time;
        temp_decoding += resp.decoding_time;
        temp_cross_cluster += resp.cross_cluster_time;
        temp_cc_transfers += resp.cross_cluster_transfers;
        temp_io_cnt += resp.io_cnt;
      } else {
        std::cout << "[Warning] Stripe " << i << " Target " << j << " repair failed (undecodable)." << std::endl;
      }
    }

    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    io_cnts.push_back(temp_io_cnt);

    std::cout << "[Stripe " << i << " Average] "
              << "repair = " << temp_repair / block_num << "s, "
              << "decoding = " << temp_decoding / block_num << "s, "
              << "cross-cluster-count = " << (double)temp_cc_transfers / block_num << ", "
              << "I/Os = " << (double)temp_io_cnt / block_num 
              << std::endl;
  }

  int total_measurements = stripe_num * block_num;
  auto avg_repair = std::accumulate(repair_times.begin(), repair_times.end(), 0.0) / total_measurements;
  auto avg_decoding = std::accumulate(decoding_times.begin(), decoding_times.end(), 0.0) / total_measurements;
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(), cross_cluster_times.end(), 0.0) / total_measurements;
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(), cross_cluster_transfers.end(), 0) / total_measurements;
  auto avg_io_cnt = (double)std::accumulate(io_cnts.begin(), io_cnts.end(), 0) / total_measurements;

  std::cout << "^-^[ " << m_name << " Maintenance Global Average ]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster time = " << avg_cross_cluster
            << "s, cross-cluster-count = " << avg_cc_transfers
            << ", I/Os = " << avg_io_cnt
            << std::endl;
}

void test_multiple_blocks_repair(Client &client, int block_num, const ParametersInfo &paras,
                                 int failed_num)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<double> meta_times;
  std::vector<int> cross_cluster_transfers;
  std::vector<int> io_cnts;
  int run_time = 5;
  int tot_cnt = 0;
  ErasureCode *ec = ec_factory(paras.ec_type, paras.cp);
  std::cout << "Multi-Block Repair:" << std::endl;
  for (int i = 0; i < stripe_num; i++)
  {
    std::cout << "[Stripe " << i << "]" << std::endl;
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    double temp_meta = 0;
    int temp_cc_transfers = 0;
    int temp_io_cnt = 0;
    int cnt = 0;
    for (int j = 0; j < run_time; j++)
    {
      // int failed_num = random_range(2, 4);
      std::vector<int> failed_blocks;
      random_n_num(0, block_num - 1, failed_num, failed_blocks);
      std::vector<unsigned int> failures;
      for (auto &block : failed_blocks)
      {
        failures.push_back((unsigned int)block);
      }
      if (!ec->check_if_decodable(failed_blocks))
      {
        j--;
        continue;
      }
      auto resp = client.blocks_repair(failures, stripe_ids[i]);
      if (resp.success)
      {
        temp_repair += resp.repair_time;
        temp_decoding += resp.decoding_time;
        temp_cross_cluster += resp.cross_cluster_time;
        temp_meta += resp.meta_time;
        temp_cc_transfers += resp.cross_cluster_transfers;
        temp_io_cnt += resp.io_cnt;
        cnt++;
      }
    }
    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    meta_times.push_back(temp_meta);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    io_cnts.push_back(temp_io_cnt);
    std::cout << "repair = " << temp_repair / cnt
              << "s, decoding = " << temp_decoding / cnt
              << "s, cross-cluster = " << temp_cross_cluster / cnt
              << "s, meta = " << temp_meta / cnt
              << "s, cross-cluster-count = " << (double)temp_cc_transfers / cnt
              << ", I/Os = " << temp_io_cnt / cnt
              << std::endl;
    tot_cnt += cnt;
  }
  auto avg_repair = std::accumulate(repair_times.begin(),
                                    repair_times.end(), 0.0) /
                    tot_cnt;
  auto avg_decoding = std::accumulate(decoding_times.begin(),
                                      decoding_times.end(), 0.0) /
                      tot_cnt;
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(),
                                           cross_cluster_times.end(), 0.0) /
                           tot_cnt;
  auto avg_meta = std::accumulate(meta_times.begin(),
                                  meta_times.end(), 0.0) /
                  tot_cnt;
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(),
                                                  cross_cluster_transfers.end(), 0) /
                          tot_cnt;
  auto avg_io_cnt = (double)std::accumulate(io_cnts.begin(),
                                            io_cnts.end(), 0) /
                    tot_cnt;
  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster = " << avg_cross_cluster
            << "s, meta = " << avg_meta
            << "s, cross-cluster-count = " << avg_cc_transfers
            << ", I/Os = " << avg_io_cnt
            << std::endl;
  if (ec != nullptr)
  {
    delete ec;
    ec = nullptr;
  }
}

void test_multiple_blocks_repair_lrc(Client &client, const ParametersInfo &paras,
                                     int failed_num)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<double> meta_times;
  std::vector<int> cross_cluster_transfers;
  std::vector<int> io_cnts;
  int run_time = 10;
  int tot_cnt = 0;
  LocallyRepairableCode *lrc = lrc_factory(paras.ec_type, paras.cp);
  std::vector<std::vector<int>> groups;
  lrc->grouping_information(groups);
  int group_num = (int)groups.size();
  std::cout << "Multi-Block Repair:" << std::endl;
  for (int i = 0; i < stripe_num; i++)
  {
    std::cout << "[Stripe " << i << "]" << std::endl;
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    double temp_meta = 0;
    int temp_cc_transfers = 0;
    int temp_io_cnt = 0;
    int cnt = 0;
    for (int j = 0; j < run_time; j++)
    {
      // int gid = random_index((size_t)group_num);
      int ran_data_idx = random_index((size_t)(lrc->k + lrc->g));
      int gid = ran_data_idx / lrc->r;
      std::vector<int> failed_blocks;
      random_n_element(2, groups[gid], failed_blocks);
      if (failed_num > 2)
      {
        int t_gid = random_index((size_t)group_num);
        int t_idx = random_index(groups[t_gid].size());
        int failed_idx = groups[t_gid][t_idx];
        while (std::find(failed_blocks.begin(), failed_blocks.end(), failed_idx) != failed_blocks.end())
        {
          t_gid = random_index((size_t)group_num);
          t_idx = random_index(groups[t_gid].size());
          failed_idx = groups[t_gid][t_idx];
        }
        failed_blocks.push_back(failed_idx);
        if (failed_num > 3)
        {
          int tt_gid = 0;
          if (gid == t_gid && paras.cp.g < 3)
          {
            tt_gid = (gid + random_index((size_t)(group_num - 1)) + 1) % group_num;
          }
          else
          {
            tt_gid = random_index((size_t)group_num);
          }
          t_idx = random_index(groups[tt_gid].size());
          failed_idx = groups[tt_gid][t_idx];
          while (std::find(failed_blocks.begin(), failed_blocks.end(), failed_idx) != failed_blocks.end())
          {
            if (gid == t_gid && paras.cp.g < 3)
            {
              tt_gid = (gid + random_index((size_t)(group_num - 1)) + 1) % group_num;
            }
            else
            {
              tt_gid = random_index((size_t)group_num);
            }
            t_idx = random_index(groups[tt_gid].size());
            failed_idx = groups[tt_gid][t_idx];
          }
          failed_blocks.push_back(failed_idx);
        }
      }
      if (!lrc->check_if_decodable(failed_blocks))
      {
        j--;
        continue;
      }
      std::vector<unsigned int> failures;
      for (auto &block : failed_blocks)
      {
        failures.push_back((unsigned int)block);
      }
      auto resp = client.blocks_repair(failures, stripe_ids[i]);
      if (resp.success)
      {
        temp_repair += resp.repair_time;
        temp_decoding += resp.decoding_time;
        temp_cross_cluster += resp.cross_cluster_time;
        temp_meta += resp.meta_time;
        temp_cc_transfers += resp.cross_cluster_transfers;
        temp_io_cnt += resp.io_cnt;
        cnt++;
      }
    }
    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    meta_times.push_back(temp_meta);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    io_cnts.push_back(temp_io_cnt);
    std::cout << "repair = " << temp_repair / cnt
              << "s, decoding = " << temp_decoding / cnt
              << "s, cross-cluster = " << temp_cross_cluster / cnt
              << "s, meta = " << temp_meta / cnt
              << "s, cross-cluster-count = " << (double)temp_cc_transfers / cnt
              << ", I/Os = " << temp_io_cnt / cnt
              << std::endl;
    tot_cnt += cnt;
  }
  auto avg_repair = std::accumulate(repair_times.begin(),
                                    repair_times.end(), 0.0) /
                    tot_cnt;
  auto avg_decoding = std::accumulate(decoding_times.begin(),
                                      decoding_times.end(), 0.0) /
                      tot_cnt;
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(),
                                           cross_cluster_times.end(), 0.0) /
                           tot_cnt;
  auto avg_meta = std::accumulate(meta_times.begin(),
                                  meta_times.end(), 0.0) /
                  tot_cnt;
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(),
                                                  cross_cluster_transfers.end(), 0) /
                          tot_cnt;
  auto avg_io_cnt = (double)std::accumulate(io_cnts.begin(),
                                            io_cnts.end(), 0) /
                    tot_cnt;
  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster = " << avg_cross_cluster
            << "s, meta = " << avg_meta
            << "s, cross-cluster-count = " << avg_cc_transfers
            << ", I/Os = " << avg_io_cnt
            << std::endl;
  if (lrc != nullptr)
  {
    delete lrc;
    lrc = nullptr;
  }
}

void test_stripe_merging(Client &client, int step_size)
{
  my_assert(step_size > 1);
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::cout << "Stripe Merging:" << std::endl;
  auto resp = client.merge(step_size);
  std::cout << "[Total]" << std::endl;
  std::cout << "merging = " << resp.merging_time
            << "s, computing = " << resp.computing_time
            << "s, cross-cluster = " << resp.cross_cluster_time
            << "s, meta =" << resp.meta_time
            << "s, cross-cluster-count = " << resp.cross_cluster_transfers
            << ", I/Os = " << resp.io_cnt
            << std::endl;
  std::cout << "[Average for every " << step_size << " stripes]" << std::endl;
  std::cout << "merging = " << resp.merging_time / stripe_num
            << "s, computing = " << resp.computing_time / stripe_num
            << "s, cross-cluster = " << resp.cross_cluster_time / stripe_num
            << "s, meta =" << resp.meta_time / stripe_num
            << "s, cross-cluster-count = "
            << (double)resp.cross_cluster_transfers / stripe_num
            << ", I/Os = " << resp.io_cnt / stripe_num
            << std::endl;
}

void generate_random_multi_block_failures_lrc(std::string filename,
                                              int stripe_num, const ParametersInfo &paras, int failed_num)
{
  LocallyRepairableCode *lrc = lrc_factory(paras.ec_type, paras.cp);
  std::vector<std::vector<int>> groups;
  lrc->grouping_information(groups);
  int group_num = (int)groups.size();
  std::string suf = lrc->type() + "_" + std::to_string(paras.cp.k) + "_" +
                    std::to_string(paras.cp.l) + "_" + std::to_string(paras.cp.g) + "_" +
                    std::to_string(failed_num);
  filename += suf;
  std::ofstream outFile(filename);
  if (!outFile)
  {
    std::cerr << "Error! Unable to open " << filename << std::endl;
    return;
  }
  int cases_per_stripe = 10;
  for (int i = 0; i < cases_per_stripe * stripe_num; i++)
  {
    int ran_data_idx = random_index((size_t)(lrc->k + lrc->g));
    int gid = ran_data_idx / lrc->r;
    std::vector<int> failed_blocks;
    random_n_element(2, groups[gid], failed_blocks);
    if (failed_num > 2)
    {
      int t_gid = random_index((size_t)group_num);
      int t_idx = random_index(groups[t_gid].size());
      int failed_idx = groups[t_gid][t_idx];
      while (std::find(failed_blocks.begin(), failed_blocks.end(), failed_idx) != failed_blocks.end())
      {
        t_gid = random_index((size_t)group_num);
        t_idx = random_index(groups[t_gid].size());
        failed_idx = groups[t_gid][t_idx];
      }
      failed_blocks.push_back(failed_idx);
      if (failed_num > 3)
      {
        int tt_gid = 0;
        if (gid == t_gid && paras.cp.g < 3)
        {
          tt_gid = (gid + random_index((size_t)(group_num - 1)) + 1) % group_num;
        }
        else
        {
          tt_gid = random_index((size_t)group_num);
        }
        t_idx = random_index(groups[tt_gid].size());
        failed_idx = groups[tt_gid][t_idx];
        while (std::find(failed_blocks.begin(), failed_blocks.end(), failed_idx) != failed_blocks.end())
        {
          if (gid == t_gid && paras.cp.g < 3)
          {
            tt_gid = (gid + random_index((size_t)(group_num - 1)) + 1) % group_num;
          }
          else
          {
            tt_gid = random_index((size_t)group_num);
          }
          t_idx = random_index(groups[tt_gid].size());
          failed_idx = groups[tt_gid][t_idx];
        }
        failed_blocks.push_back(failed_idx);
      }
    }
    if (!lrc->check_if_decodable(failed_blocks))
    {
      i--;
      continue;
    }
    else
    {
      for (const auto &num : failed_blocks)
      {
        outFile << num << " ";
      }
      outFile << "\n";
    }
  }
  outFile.close();
}

void test_multiple_blocks_repair_lrc_with_testcases(std::string filename,
                                                    Client &client, const ParametersInfo &paras, int failed_num)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<double> meta_times;
  std::vector<int> cross_cluster_transfers;
  std::vector<int> io_cnts;
  int run_time = 10;
  int tot_cnt = 0;
  LocallyRepairableCode *lrc = lrc_factory(paras.ec_type, paras.cp);
  std::vector<std::vector<int>> groups;
  lrc->grouping_information(groups);
  int group_num = (int)groups.size();
  std::string suf = lrc->type() + "_" + std::to_string(paras.cp.k) + "_" +
                    std::to_string(paras.cp.l) + "_" + std::to_string(paras.cp.g) + "_" +
                    std::to_string(failed_num);
  filename += suf;
  std::ifstream inFile(filename);
  if (!inFile)
  {
    std::cerr << "Error! Unable to open " << filename << std::endl;
    return;
  }
  std::string line;
  std::cout << "Multi-Block Repair:" << std::endl;
  int ii = 0;
  int test_stripe_num = 5;
  for (int i = 0; i < test_stripe_num; i++)
  {
    std::cout << "[Stripe " << i << "]" << std::endl;
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    double temp_meta = 0;
    int temp_cc_transfers = 0;
    int temp_io_cnt = 0;
    int cnt = 0;
    for (int j = 0; j < run_time; j++)
    {
      std::vector<int> failed_blocks;
      std::getline(inFile, line);
      std::istringstream lineStream(line);
      int num;
      while (lineStream >> num)
      {
        failed_blocks.push_back(num);
      }
      if (i < test_stripe_num - stripe_num)
      {
        continue;
      }
      std::vector<unsigned int> failures;
      for (auto &block : failed_blocks)
      {
        failures.push_back((unsigned int)block);
      }
      auto resp = client.blocks_repair(failures, stripe_ids[ii]);
      if (resp.success)
      {
        temp_repair += resp.repair_time;
        temp_decoding += resp.decoding_time;
        temp_cross_cluster += resp.cross_cluster_time;
        temp_meta += resp.meta_time;
        temp_cc_transfers += resp.cross_cluster_transfers;
        temp_io_cnt += resp.io_cnt;
        cnt++;
      }
    }
    if (i < test_stripe_num - stripe_num)
    {
      continue;
    }
    ii++;
    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    meta_times.push_back(temp_meta);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    io_cnts.push_back(temp_io_cnt);
    std::cout << "repair = " << temp_repair / cnt
              << "s, decoding = " << temp_decoding / cnt
              << "s, cross-cluster = " << temp_cross_cluster / cnt
              << "s, meta = " << temp_meta / cnt
              << "s, cross-cluster-count = " << (double)temp_cc_transfers / cnt
              << ", I/Os = " << temp_io_cnt / cnt
              << std::endl;
    tot_cnt += cnt;
  }
  inFile.close();
  auto avg_repair = std::accumulate(repair_times.begin(),
                                    repair_times.end(), 0.0) /
                    tot_cnt;
  auto avg_decoding = std::accumulate(decoding_times.begin(),
                                      decoding_times.end(), 0.0) /
                      tot_cnt;
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(),
                                           cross_cluster_times.end(), 0.0) /
                           tot_cnt;
  auto avg_meta = std::accumulate(meta_times.begin(),
                                  meta_times.end(), 0.0) /
                  tot_cnt;
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(),
                                                  cross_cluster_transfers.end(), 0) /
                          tot_cnt;
  auto avg_io_cnt = (double)std::accumulate(io_cnts.begin(),
                                            io_cnts.end(), 0) /
                    tot_cnt;
  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster = " << avg_cross_cluster
            << "s, meta = " << avg_meta
            << "s, cross-cluster-count = " << avg_cc_transfers
            << ", I/Os = " << avg_io_cnt
            << std::endl;
  if (lrc != nullptr)
  {
    delete lrc;
    lrc = nullptr;
  }
}


void test_pure_read_latency(Client &client, const std::vector<std::vector<std::string>> &ms_object_keys, int stripe_num, int k) 
{
  std::cout << "\n=========================================" << std::endl;
  std::cout << "    FULL-SCALE LATENCY EVALUATION   " << std::endl;
  std::cout << "=========================================\n" << std::endl;

  std::vector<double> latencies;
  int total_blocks = stripe_num * k;

  std::cout << "\n>>> Executing " << total_blocks << " precise reads..." << std::endl;
  
  auto start_total = std::chrono::steady_clock::now();

  for (int i = 0; i < stripe_num; i++) {
    std::cout << "[Stripe " << i << "]" << std::endl;
    double temp_stripe_latency = 0.0;
    for (int j = 0; j < k; j++) {
      std::string target_key = ms_object_keys[i][j]; 
      
      auto start_single = std::chrono::steady_clock::now();
      auto value = client.get(target_key); 
      auto end_single = std::chrono::steady_clock::now();
      
      std::chrono::duration<double, std::milli> elapsed_single = end_single - start_single;
      latencies.push_back(elapsed_single.count());
      temp_stripe_latency += elapsed_single.count();
    }
    std::cout << "read latency = " << temp_stripe_latency / k << " ms" << std::endl;
  }

  auto end_total = std::chrono::steady_clock::now();
  std::chrono::duration<double> elapsed_total = end_total - start_total;

  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "[Result] Total Read Time: " << elapsed_total.count() << " s" << std::endl;
  std::cout << "[Result] Average Latency: " << (elapsed_total.count() * 1000.0) / total_blocks << " ms" << std::endl;

  // Write all data to a fixed temporary file
  std::string out_filename = "res/raw_latencies.csv";
  std::ofstream outfile(out_filename);
  if (outfile.is_open()) {
      outfile << "Latency_ms\n";
      for (double lat : latencies) {
          outfile << lat << "\n";
      }
      outfile.close();
      std::cout << ">>> Saved " << total_blocks << " records to " << out_filename << " <<<" << std::endl;
      std::cout << ">>> IMPORTANT: Please rename this file (e.g., baseline.csv) before the next run! <<<" << std::endl;
  } else {
      std::cerr << "[Error] Failed to open " << out_filename << " for writing." << std::endl;
  }
}



std::vector<int> load_trace_file(const std::string& filename) {
    std::vector<int> trace_seq;
    std::ifstream infile(filename);
    if (!infile.is_open()) {
        std::cerr << "Cannot find trace.txt! Please check the file path." << std::endl;
        exit(1);
    }
    int key_index;
    while (infile >> key_index) {
        trace_seq.push_back(key_index);
    }
    std::cout << ">>> Loaded " << trace_seq.size() << " GET requests!" << std::endl;
    return trace_seq;
}

// (maintenance_type: 3=Rack, 4=Zone)
void test_foreground_interference(Client &client, const std::vector<std::vector<std::string>> &ms_object_keys, int stripe_num, int k, int maintenance_type) 
{
    std::string m_name = (maintenance_type == 3) ? "Rack" : "Zone";
    std::cout << "\n=========================================" << std::endl;
    std::cout << "  FOREGROUND INTERFERENCE (" << m_name << " MAINTENANCE) " << std::endl;
    std::cout << "=========================================\n" << std::endl;

    std::atomic<bool> keep_running(true);
    std::atomic<int> current_iops(0);

    // 1. Flatten all keys for the foreground threads to access
    std::vector<std::string> all_keys;
    for (const auto& keys : ms_object_keys) {
        for (const auto& current_k : keys) {
            all_keys.push_back(current_k);
        }
    }

    // =====================================================================
    // TRACE INTEGRATION: Load the script and define the pointer outside the threads
    // =====================================================================
    int total_keys = all_keys.size();
    // Assuming the trace file is in the same directory. Adjust path if needed.
    std::vector<int> global_trace = load_trace_file("data/trace.txt"); 
    std::atomic<size_t> trace_ptr(0); 

    // 2. Define the foreground worker thread (infinite loop for trace reading)
    auto foreground_worker = [&](int thread_id) {

        int unique_client_port = CLIENT_PORT + thread_id + 1;
        Client local_client("192.168.0.218", unique_client_port, "192.168.0.218", COORDINATOR_PORT);
        
        while (keep_running) {
            // Fetch the next line from the trace atomically
            size_t current_idx = trace_ptr.fetch_add(1);
            
            // Loop back to the start if we reach the end of the trace
            if (current_idx >= global_trace.size()) {
                current_idx = current_idx % global_trace.size();
            }

            // Get the target ID from the trace script
            int target_trace_id = global_trace[current_idx];

            // Safety boundary to prevent out-of-bounds access
            int safe_key_idx = target_trace_id % total_keys;

            // Execute the GET request based on the skewed trace
            auto value = local_client.get(all_keys[safe_key_idx]); 
            current_iops++;
        }
    };

    // 3. Define the monitor thread
    std::string out_filename = "res/trace_" + m_name + ".csv";
    auto monitor_worker = [&]() {
        std::ofstream outfile(out_filename);
        outfile << "Time_s,IOPS\n";
        int time_sec = 0;
        while (keep_running) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            time_sec++;
            int ops_this_sec = current_iops.exchange(0);
            std::cout << "[Monitor] Time: " << time_sec << "s | Foreground IOPS: " << ops_this_sec << std::endl;
            outfile << time_sec << "," << ops_this_sec << "\n";
        }
        outfile.close();
    };

    // Start multi-threading
    int num_fg_threads = 4;
    std::vector<std::thread> fg_threads;
    for (int i = 0; i < num_fg_threads; i++) {
        // Pass i (thread_id) to the Lambda to initialize a dedicated Client
        fg_threads.emplace_back(foreground_worker, i); 
    }
    std::thread monitor_thread(monitor_worker);

    std::cout << "\n>>> [Phase 1] Running steady state for 10 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(10));

    std::cout << "\n>>> [Phase 2] Triggering Background " << m_name << " Maintenance (Worker Pool)!" << std::endl;
    auto bg_start = std::chrono::steady_clock::now();
    
    auto stripe_ids = client.list_stripes(); 
    int total_stripes_available = stripe_ids.size();

    int total_stripes_to_repair = std::min(6, total_stripes_available); 
    int bg_concurrency_limit = 1; 

    struct RepairTask {
        int stripe_id; 
        unsigned int failed_block_idx;
    };
    std::vector<RepairTask> random_tasks;
     
    srand(2026); 
    
    for (int i = 0; i < total_stripes_to_repair; i++) {
        int rand_stripe_idx = rand() % total_stripes_available;
        unsigned int rand_failed_block = rand() % k; 
        
        RepairTask task;
        task.stripe_id = stripe_ids[rand_stripe_idx];
        task.failed_block_idx = rand_failed_block;
        random_tasks.push_back(task);
    }
    // =========================================================================

    std::atomic<int> next_task_idx(0);
    std::vector<std::thread> bg_workers;

    for (int i = 0; i < bg_concurrency_limit; i++) {
        bg_workers.emplace_back([random_tasks, total_stripes_to_repair, maintenance_type, &next_task_idx, i]() {
            int unique_bg_port = CLIENT_PORT + 100 + i; 
            Client bg_client("192.168.0.218", unique_bg_port, "192.168.0.218", COORDINATOR_PORT);
            
            while (true) {
                int current_task = next_task_idx.fetch_add(1);
                
                if (current_task >= total_stripes_to_repair) {
                    break;
                }

                RepairTask task = random_tasks[current_task];
                std::vector<unsigned int> target_failures = {task.failed_block_idx}; 
                
                bg_client.blocks_repair(target_failures, task.stripe_id, maintenance_type);
            }
        });
    }

    // Main thread blocks, waiting for all workers to finish the tasks
    for(auto& t : bg_workers) {
        if (t.joinable()) t.join();
    }

    auto bg_end = std::chrono::steady_clock::now();
    std::chrono::duration<double> bg_duration = bg_end - bg_start;
    std::cout << "\n>>> [Info] Background maintenance finished in " << bg_duration.count() << " seconds!" << std::endl;

    std::cout << "\n>>> [Phase 3] Running recovery state for 10 seconds..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(19));

    // Stop and join threads
    keep_running = false;
    for (auto& t : fg_threads) {
        if (t.joinable()) t.join();
    }
    if (monitor_thread.joinable()) monitor_thread.join();

    std::cout << ">>> Experiment completed! Data saved to " << out_filename << std::endl;
}


void test_throughput_saturation(Client &client, const std::vector<std::vector<std::string>> &ms_object_keys) 
{
    std::cout << "\n=========================================" << std::endl;
    std::cout << "  FOREGROUND SATURATION SWEEP TEST  " << std::endl;
    std::cout << "=========================================\n" << std::endl;

    std::vector<std::string> all_keys;
    for (const auto& keys : ms_object_keys) {
        for (const auto& k : keys) {
            all_keys.push_back(k);
        }
    }
    int total_keys = all_keys.size();

    std::vector<int> thread_counts = {1, 2, 4, 6, 8, 10, 12, 16};
    int test_duration_sec = 10; 

    std::string out_filename = "res/saturation_sweep.csv";
    std::ofstream outfile(out_filename);
    outfile << "Thread_Count,Average_IOPS\n";

    for (int tc : thread_counts) {
        std::cout << ">>> Testing with " << tc << " concurrent thread(s) for " << test_duration_sec << " seconds..." << std::endl;

        std::atomic<bool> keep_running(true);
        std::atomic<int> total_ops(0);
        std::vector<std::thread> fg_threads;

        for (int i = 0; i < tc; i++) {
            fg_threads.emplace_back([&, i]() {
                int unique_client_port = CLIENT_PORT + 200 + i; 
                Client local_client("192.168.0.218", unique_client_port, "192.168.0.218", COORDINATOR_PORT);
                
                while (keep_running) {
                    int rand_idx = rand() % total_keys;
                    local_client.get(all_keys[rand_idx]);
                    total_ops++;
                }
            });
        }

        std::this_thread::sleep_for(std::chrono::seconds(test_duration_sec));

        keep_running = false;
        for (auto& t : fg_threads) {
            if (t.joinable()) t.join();
        }

        double avg_iops = static_cast<double>(total_ops) / test_duration_sec;
        std::cout << "[Result] Threads: " << tc << " | Average IOPS: " << avg_iops << "\n" << std::endl;
        
        outfile << tc << "," << avg_iops << "\n";
        std::this_thread::sleep_for(std::chrono::seconds(2)); 
    }

    outfile.close();
    std::cout << ">>> Saturation sweep completed! Data saved to " << out_filename << std::endl;
}


void test_repair_performance(
    std::string path_prefix, int stripe_num, const ParametersInfo &paras, int failed_num, bool optimized)
{
  int block_num = paras.cp.k + paras.cp.m;

  Client client("192.168.0.218", CLIENT_PORT, "192.168.0.218", COORDINATOR_PORT);

  // set erasure coding parameters
  client.set_ec_parameters(paras);

  struct timeval start_time, end_time;
  // generate key-value pair
  std::vector<size_t> value_lengths(stripe_num, 0);
  std::vector<std::vector<std::string>> ms_object_keys;
  std::vector<std::vector<size_t>> ms_object_sizes;
  std::vector<std::vector<unsigned int>> ms_object_accessrates;
  // std::vector<std::vector<std::string>> ms_object_values;
  int obj_id = 0;
  for (int i = 0; i < stripe_num; i++)
  {
    std::vector<std::string> object_keys;
    for (int j = 0; j < paras.cp.k; j++)
    {
      value_lengths[i] += paras.block_size;
      object_keys.emplace_back("obj" + std::to_string(obj_id++));
    }
    ms_object_keys.emplace_back(object_keys);
    ms_object_sizes.emplace_back(std::vector<size_t>(paras.cp.k, paras.block_size));
    ms_object_accessrates.emplace_back(std::vector<unsigned int>(paras.cp.k, 1));
  }
#ifdef IN_MEMORY
  std::unordered_map<std::string, std::string> key_value;
  generate_unique_random_strings_difflen(5, stripe_num, value_lengths, key_value);
#endif

  // set
  double set_time = 0;
#ifdef IN_MEMORY
  int i = 0;
  for (auto &kv : key_value)
  {
    gettimeofday(&start_time, NULL);
    double encoding_time = client.set(kv.second, ms_object_keys[i], ms_object_sizes[i],
                                      ms_object_accessrates[i]);
    gettimeofday(&end_time, NULL);
    double temp_time = end_time.tv_sec - start_time.tv_sec +
                       (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
    set_time += temp_time;
    std::cout << "[SET] set time: " << temp_time << ", encoding time: "
              << encoding_time << std::endl;
    ++i;
  }
  std::cout << "Total set time: " << set_time << ", average set time:"
            << set_time / stripe_num << std::endl;
  std::cout << "Write Throughput: " << paras.block_size * paras.cp.k * stripe_num / (set_time * 1024)
            << " KB/s" << std::endl;
#else
  for (int i = 0; i < stripe_num; i++)
  {
    std::string readpath = path_prefix + "/../../data/Object";
    double encoding_time = 0;
    gettimeofday(&start_time, NULL);
    if (access(readpath.c_str(), 0) == -1)
    {
      std::cout << "[Client] file does not exist!" << std::endl;
      exit(-1);
    }
    else
    {
      char *buf = new char[value_lengths[i]];
      std::ifstream ifs(readpath);
      ifs.read(buf, value_lengths[i]);
      encoding_time = client.set(std::string(buf, value_lengths[i]), ms_object_keys[i],
                                 ms_object_sizes[i], ms_object_accessrates[i]);
      ifs.close();
      // std::vector<std::string> object_values;
      // for (int j = 0; j < paras.cp.k; j++) {
      //   object_values.emplace_back(std::string(buf + j * paras.block_size, paras.block_size));
      // }
      // ms_object_values.emplace_back(object_values);
      delete buf;
    }
    gettimeofday(&end_time, NULL);
    double temp_time = end_time.tv_sec - start_time.tv_sec +
                       (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
    set_time += temp_time;
    std::cout << "[SET] set time: " << temp_time << ", encoding time: "
              << encoding_time << std::endl;
  }
  std::cout << "Total set time: " << set_time << ", average set time:"
            << set_time / stripe_num << std::endl;
  std::cout << "Write Throughput: " << paras.block_size * paras.cp.k * stripe_num / (set_time * 1024)
            << " KB/s" << std::endl;
#endif

  /*
    // get
    double get_time = 0.0;
    auto start = std::chrono::steady_clock::now();
    for (auto &object_keys : ms_object_keys)
    {
      for (auto &key : object_keys)
      {
        auto value = client.get(key);
        // my_assert(value == ms_object_values[i][j]);
      }
    }
    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    get_time = elapsed.count();
    std::cout << "Total get time: " << get_time << " , average get time:"
              << double(get_time) / double(stripe_num * paras.cp.k) << std::endl;
    std::cout << "Read Throughput: " << double(paras.block_size * paras.cp.k * stripe_num) / double(get_time * 1024)
              << " MB/s" << std::endl;
  */

  bool is_ec = paras.is_ec_now;

  // redundancy transitioning, from replicas to ec
  if (!is_ec)
  {
    auto resp = client.redundancy_transition(optimized);
    if (resp.iftransed)
    {
      std::cout << "[Rep->EC] transition time: " << resp.transition_time
                << ", encoding time: " << resp.encoding_time
                << ", cross-cluster time: " << resp.cross_cluster_time
                << ", meta time: " << resp.meta_time
                << ", cross-cluster-count: " << resp.cross_cluster_transfers
                << "(data=" << resp.data_reloc_cnt
                << ", parity=" << resp.parity_reloc_cnt
                << "), I/Os = " << resp.io_cnt
                << std::endl;
      std::cout << "[Average for every stripes]" << std::endl;
      std::cout << "[Rep->EC] transition time: " << resp.transition_time / stripe_num
                << ", encoding time: " << resp.encoding_time / stripe_num
                << ", cross-cluster time: " << resp.cross_cluster_time / stripe_num
                << ", meta time: " << resp.meta_time / stripe_num
                << ", cross-cluster-count: " << resp.cross_cluster_transfers / stripe_num
                << "(data=" << resp.data_reloc_cnt / stripe_num
                << ", parity=" << resp.parity_reloc_cnt / stripe_num
                << "), I/Os = " << resp.io_cnt / stripe_num
                << std::endl;
      std::cout << "[Max] transition time: " << resp.max_trans_time
                << ", [Min] transition time: " << resp.min_trans_time << std::endl;
    }
    else
    {
      std::cout << "[Rep->EC] failed!" << std::endl;
    }
    is_ec = true;
  }

  // test repair
  if (is_ec)
  {
    if (failed_num == 1)
    {
      test_single_block_repair(client, block_num);
    }
    else if (failed_num == 2)
    {
      test_multiple_blocks_repair(client, block_num, paras, failed_num);
    }
    else if (failed_num == 3) {
      // rack/node
      test_maintenance_repair(client, block_num, 3);
    }
    else if (failed_num == 4) {
      test_maintenance_repair(client, block_num, 4); 
    }

    else if(failed_num == 5) {
      test_pure_read_latency(client, ms_object_keys, stripe_num, paras.cp.k);
    }
    else if (failed_num == 6) {
      test_foreground_interference(client, ms_object_keys, stripe_num, paras.cp.k, 3);
    }
    else if (failed_num == 7) {
      test_foreground_interference(client, ms_object_keys, stripe_num, paras.cp.k, 4);
    }
    else if (failed_num == 8) {
      test_throughput_saturation(client, ms_object_keys);
    }
  }

  // delete
  client.delete_all_stripes();
}

int main(int argc, char **argv)
{
  if (argc != 4 && argc != 5)
  {
    std::cout << "./run_client config_file stripe_num failed_num" << std::endl;
    exit(0);
  }

  char buff[256];
  getcwd(buff, 256);
  std::string cwf = std::string(argv[0]);
  std::string path_prefix = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1);

  ParametersInfo paras;
  parse_args(nullptr, paras, path_prefix + "/../" + std::string(argv[1]));
  int stripe_num = std::stoi(argv[2]);

  int failed_num = std::stoi(argv[3]);
  my_assert(0 <= failed_num && failed_num <= 8);
  bool optimized = true;
  if (argc == 5)
  {
    optimized = (std::string(argv[4]) == "true");
  }

  test_repair_performance(path_prefix, stripe_num, paras, failed_num, optimized);

  return 0;
}
