#include "rs.h"

using namespace ECProject;

void RSCode::make_encoding_matrix(int *final_matrix)
{
	int *matrix = reed_sol_vandermonde_coding_matrix(k, m, w);

	bzero(final_matrix, sizeof(int) * k * m);

	for (int i = 0; i < m; i++) {
		for (int j = 0; j < k; j++) {
			final_matrix[i * k + j] = matrix[i * k + j];
		}
	}

	free(matrix);
}

void RSCode::encode(char **data_ptrs, char **coding_ptrs, int block_size)
{
	std::vector<int> rs_matrix(k * m, 0);
	make_encoding_matrix(rs_matrix.data());
	jerasure_matrix_encode(k, m, w, rs_matrix.data(), data_ptrs, coding_ptrs, block_size);
}

void RSCode::decode(char **data_ptrs, char **coding_ptrs, int block_size,
										int *erasures, int failed_num)
{
	if (failed_num > m) {
		std::cout << "[Decode] Undecodable!" << std::endl;
		return;
	}
	int *rs_matrix = reed_sol_vandermonde_coding_matrix(k, m, w);
	int ret = 0;
	ret = jerasure_matrix_decode(k, m, w, rs_matrix, failed_num, erasures,
															 data_ptrs, coding_ptrs, block_size);
	if (ret == -1) {
		std::cout << "[Decode] Failed!" << std::endl;
		return;
	}
}

void RSCode::encode_partial_blocks(
				char **data_ptrs, char **coding_ptrs, int block_size,
				const std::vector<int>& data_idxs, const std::vector<int>& parity_idxs,
				const std::vector<int>& failure_idxs, const std::vector<int>& live_idxs,
				std::vector<bool>& partial_flags, bool partial_scheme)
{
	if (partial_scheme) {
		if (parity_idxs.size() == failure_idxs.size()) {
			std::vector<int> rs_matrix((k + m) * (k + m), 0);
			get_identity_matrix(rs_matrix.data(), k + m, k + m);
			make_full_matrix(&(rs_matrix.data())[k * (k + m)], k + m);
			// print_matrix(rs_matrix.data(), k + m, k + m, "full_matrix");
			encode_partial_blocks_for_failures_(k + m, rs_matrix.data(), data_ptrs,
					coding_ptrs, block_size, data_idxs, parity_idxs, failure_idxs);
		} else {
			std::vector<int> rs_matrix((k + m) * k, 0);
			get_identity_matrix(rs_matrix.data(), k, k);
			make_encoding_matrix(&(rs_matrix.data())[k * k]);
			encode_partial_blocks_for_failures_v2_(k, rs_matrix.data(), data_ptrs,
																						 coding_ptrs, block_size,
																						 data_idxs, failure_idxs,
																						 live_idxs); 
		}
		
	} else {
		int parity_num = (int)parity_idxs.size();
		for (int i = 0; i < parity_num; i++) {
			my_assert(parity_idxs[i] >= k);
			partial_flags[i] = true;
		}
		std::vector<int> rs_matrix((k + m) * (k + m), 0);
		get_identity_matrix(rs_matrix.data(), k + m, k + m);
		make_full_matrix(&(rs_matrix.data())[k * (k + m)], k + m);
		// print_matrix(rs_matrix.data(), k + m, k + m, "full_matrix");
		encode_partial_blocks_for_parities_(k + m, rs_matrix.data(), data_ptrs,
				coding_ptrs, block_size, data_idxs, parity_idxs);
	}
	
}

void RSCode::decode_with_partial_blocks(
				char **data_ptrs, char **coding_ptrs, int block_size,
				const std::vector<int>& failure_idxs, const std::vector<int>& parity_idxs)
{
	for (auto idx : parity_idxs) {
		my_assert(idx >= k);
	}
	std::vector<int> rs_matrix((k + m) * (k + m), 0);
	get_identity_matrix(rs_matrix.data(), k + m, k + m);
	make_full_matrix(&(rs_matrix.data())[k * (k + m)], k + m);
	// print_matrix(rs_matrix.data(), k + m, k + m, "full_matrix");
	decode_with_partial_blocks_(k + m, rs_matrix.data(), data_ptrs, coding_ptrs,
															block_size, failure_idxs, parity_idxs);
}

int RSCode::num_of_partial_blocks_to_transfer(
				const std::vector<int>& data_idxs, const std::vector<int>& parity_idxs)
{
	return (int)parity_idxs.size();
}

bool RSCode::check_if_decodable(const std::vector<int>& failure_idxs)
{
	int failed_num = (int)failure_idxs.size();
	if (m >= failed_num) {
		return true;
	} else {
		return false;
	}
}

void RSCode::partition_random()
{
	int n = k + m;
	std::vector<int> blocks;
	for (int i = 0; i < n; i++) {
		blocks.push_back(i);
	}

	int cnt = 0;
	int cnt_left = n;
	while (cnt < n) {
		// at least subject to single-region fault tolerance
		int random_partition_size = random_range(1, m);
		int partition_size = std::min(random_partition_size, n - cnt);
		std::vector<int> partition;
		for (int i = 0; i < partition_size; i++, cnt++) {
			int ran_idx = random_index(n - cnt);
			int block_idx = blocks[ran_idx];
			partition.push_back(block_idx);
			auto it = std::find(blocks.begin(), blocks.end(), block_idx);
			blocks.erase(it);
		}
		partition_plan.push_back(partition);
	}
}

void RSCode::partition_optimal()
{
	int n = k + m;
	int cnt = 0;
	while (cnt < n) {
		// every m blocks in a partition
		int partition_size = std::min(m, n - cnt);
		std::vector<int> partition;
		for (int i = 0; i < partition_size; i++, cnt++) {
			partition.push_back(cnt);
		}
		partition_plan.push_back(partition);
	}
}

std::string RSCode::self_information()
{
	return "RS(" + std::to_string(k) + "," + std::to_string(m) + ")";
}

std::string RSCode::type()
{
	return "RS";
}

void RSCode::help_blocks_for_single_block_repair(
				int failure_idx,
				std::vector<int>& parity_idxs,
				std::vector<std::vector<int>>& help_blocks,
				bool partial_scheme)
{
	int parition_num = (int)partition_plan.size();
	if (!parition_num) {
    return;
  }
	if (failure_idx >= k) {
		parity_idxs.push_back(failure_idx);
	}

	if (failure_idx < k || (partial_scheme && failure_idx >= k)) {
		int main_parition_idx = -1;
		std::vector<std::pair<int, int>> partition_idx_to_num;
		for (int i = 0; i < parition_num; i++) {
			auto it = std::find(partition_plan[i].begin(), partition_plan[i].end(),
													failure_idx);
			if (it != partition_plan[i].end()) {
				main_parition_idx = i;
			} else {
				int partition_size = (int)partition_plan[i].size();
				partition_idx_to_num.push_back(std::make_pair(i, partition_size));
			}
		}
		std::sort(partition_idx_to_num.begin(), partition_idx_to_num.end(),
							cmp_descending);

		int cnt = 0;
		std::vector<int> main_help_block;
		for (auto idx : partition_plan[main_parition_idx]) {
			if (idx != failure_idx) {
				if (cnt < k) {
					main_help_block.push_back(idx);
					cnt++;
					if (idx >= k) {
						parity_idxs.push_back(idx);
					}
				} else {
					break;
				}
			}
		}
		if (cnt > 0) {
			help_blocks.push_back(main_help_block);
		}
		if (cnt == k) {
			return;
		}
		for (auto& pair : partition_idx_to_num) {
			std::vector<int> help_block;
			for (auto idx : partition_plan[pair.first]) {
				if (cnt < k) {
					help_block.push_back(idx);
					cnt++;
					if (idx >= k) {
						parity_idxs.push_back(idx);
					}
				} else {
					break;
				}
			}
			if (cnt > 0 && cnt <= k) {
				help_blocks.push_back(help_block);
			} 
			if (cnt == k) {
				return;
			}
		}
	} else {
		for (auto partition : partition_plan) {
      std::vector<int> help_block;
      for (auto bid : partition) {
      	if (bid < k) {
          help_block.push_back(bid);
        }
      }
      if (help_block.size() > 0) {
        help_blocks.push_back(help_block);
      }
    }
	}
}

void RSCode::help_blocks_for_multi_blocks_repair(
				std::vector<int> failure_idxs,
				std::vector<int>& parity_idxs,
				std::vector<std::vector<int>>& help_blocks,
				bool partial_scheme)
{
	int parition_num = (int)partition_plan.size();
	if (!parition_num) {
    return;
  }
	bool global_flag = true;
	for (auto failure_idx : failure_idxs) {
		if (failure_idx < k) {
			global_flag = false;
			break;
		}
		if (failure_idx >= k) {
			parity_idxs.push_back(failure_idx);
		}
	}

	if (global_flag && !partial_scheme) {
		for (auto partition : partition_plan) {
      std::vector<int> help_block;
      for (auto bid : partition) {
        if (bid < k) {
          help_block.push_back(bid);
        }
      }
      if (help_block.size() > 0) {
        help_blocks.push_back(help_block);
      }
    }
	} else {
		std::vector<std::vector<int>> copy_partition;
		for (int i = 0; i < parition_num; i++) {
			std::vector<int> partition;
			for (auto idx : partition_plan[i]) {
				partition.push_back(idx);
			}
			copy_partition.push_back(partition);
		}

		int failures_cnt[parition_num] = {0};
		for (auto failure_idx : failure_idxs) {
			for (int i = 0; i < parition_num; i++) {
				auto it = std::find(copy_partition[i].begin(), copy_partition[i].end(),
														failure_idx);
				if (it != copy_partition[i].end()) {
					failures_cnt[i]++;
					copy_partition[i].erase(it);	// remove the failures
					break;
				}			
			}
		}
		std::vector<std::pair<int, int>> main_partition_idx_to_num;
		std::vector<std::pair<int, int>> partition_idx_to_num;
		for (int i = 0; i < parition_num; i++) {
			int partition_size = (int)copy_partition[i].size();
			if (failures_cnt[i]) {
				main_partition_idx_to_num.push_back(std::make_pair(i, partition_size));
			} else {
				partition_idx_to_num.push_back(std::make_pair(i, partition_size));
			}
		}
		std::sort(main_partition_idx_to_num.begin(), main_partition_idx_to_num.end(),
							cmp_descending);
		std::sort(partition_idx_to_num.begin(), partition_idx_to_num.end(),
							cmp_descending);

		int cnt = 0;
		for (auto& pair : main_partition_idx_to_num) {
			std::vector<int> main_help_block;
			for (auto idx : copy_partition[pair.first]) {
				if (cnt < k) {
					main_help_block.push_back(idx);
					cnt++;
					if (idx >= k) {
						parity_idxs.push_back(idx);
					}
				} else {
					break;
				}
			}
			if (cnt > 0 && cnt <= k && main_help_block.size() > 0) {
				help_blocks.push_back(main_help_block);
			}
			if (cnt == k) {
				return;
			}
		}
		for (auto& pair : partition_idx_to_num) {
			std::vector<int> help_block;
			for (auto idx : copy_partition[pair.first]) {
				if (cnt < k) {
					help_block.push_back(idx);
					cnt++;
					if (idx >= k) {
						parity_idxs.push_back(idx);
					}
				} else {
					break;
				}
			}
			if (cnt > 0 && cnt <= k && help_block.size() > 0) {
					help_blocks.push_back(help_block);
			} 
			if (cnt == k) {
				return;
			}
		}
	}
}

bool RSCode::generate_repair_plan(const std::vector<int>& failure_idxs,
																	std::vector<RepairPlan>& plans,
																	bool partial_scheme,
																	bool repair_priority,
																	bool repair_method)
{
	int failed_num = (int)failure_idxs.size();
	RepairPlan plan;
	for (auto idx : failure_idxs) {
		plan.failure_idxs.push_back(idx);
	}
	if (failed_num == 1) {
		help_blocks_for_single_block_repair(failure_idxs[0],
																				plan.parity_idxs,
																				plan.help_blocks,
																				partial_scheme);
	} else {
		help_blocks_for_multi_blocks_repair(failure_idxs,
																				plan.parity_idxs,
																				plan.help_blocks,
																				partial_scheme);
	}
	plans.push_back(plan);
	return true;
}