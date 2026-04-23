#pragma once

#include "jerasure.h"
#include "reed_sol.h"
#include "cauchy.h"
#include "utils.h"

namespace ECProject
{
	enum ECFAMILY
  {
    RSCodes,
    LRCs,
		PCs
  };

  enum ECTYPE
  {
    RS,
    AZURE_LRC,
    AZURE_LRC_1,
    OPTIMAL_LRC,
    OPTIMAL_CAUCHY_LRC,
    UNIFORM_CAUCHY_LRC,
		NON_UNIFORM_LRC,
		PC,
		HV_PC
  };

	enum PlacementRule
	{
		FLAT,
		RANDOM,
		OPTIMAL,
		ROBUST
	};

	struct CodingParameters
  {
		PlacementRule placement_rule = OPTIMAL;
    int k;
    int m;
    int l;
    int g;
		int k1;
    int m1;
    int k2;
    int m2;
		int x;
		int seri_num = 0;
		float storage_overhead;
		bool local_or_column = false;
		std::vector<std::pair<int, int>> krs;
  };

	struct RepairPlan
	{
		bool local_or_column = false;
		std::vector<int> failure_idxs;
		std::vector<int> parity_idxs;
		std::vector<std::vector<int>> help_blocks;
	};
	
	class ErasureCode
	{
	public:
		int k;		 /* number of data blocks */
		int m;		 /* number of parity blocks */
		int w = 8; /* word size for encoding */
		float storage_overhead = 0;
		PlacementRule placement_rule = OPTIMAL;
		bool local_or_column = false;
		std::vector<std::vector<int>> partition_plan;

		ErasureCode() = default;
		ErasureCode(int k, int m) : k(k), m(m) { storage_overhead = float(k + m) / float(k); }
		virtual ~ErasureCode() {}

		virtual void init_coding_parameters(CodingParameters cp);
		virtual void get_coding_parameters(CodingParameters& cp);

		virtual void encode(char **data_ptrs, char **coding_ptrs, int block_size) = 0;
		virtual void decode(char **data_ptrs, char **coding_ptrs, int blocksize,
												int *erasures, int failed_num) = 0;
		virtual bool check_if_decodable(const std::vector<int>& failure_idxs) = 0;
		virtual void make_encoding_matrix(int *final_matrix) {}
		virtual void encode_partial_blocks(
				char **data_ptrs, char **coding_ptrs, int block_size,
				const std::vector<int>& data_idxs, const std::vector<int>& parity_idxs,
				const std::vector<int>& failure_idxs, const std::vector<int>& live_idxs,
				std::vector<bool>& partial_flags, bool partial_scheme) = 0;
		virtual void decode_with_partial_blocks(
				char **data_ptrs, char **coding_ptrs, int block_size,
				const std::vector<int>& failure_idxs, const std::vector<int>& parity_idxs) = 0;
		virtual int num_of_partial_blocks_to_transfer(
				const std::vector<int>& data_idxs, const std::vector<int>& parity_idxs) = 0;

		void print_matrix(int *matrix, int rows, int cols, std::string msg);
		void get_identity_matrix(int *matrix, int rows, int kk);
		virtual void make_full_matrix(int *matrix, int kk);
		void make_submatrix_by_rows(int cols, int *matrix, int *new_matrix,
																std::vector<int> block_idxs);
		void make_submatrix_by_cols(int cols, int rows, int *matrix, int *new_matrix,
																std::vector<int> blocks_idxs);
		void perform_addition(char **data_ptrs, char **coding_ptrs, int block_size,
													const std::vector<int>& data_idxs,
													const std::vector<int>& parity_idxs);
		void encode_partial_blocks_for_parities_(int k_, int *full_matrix,
																char **data_ptrs, char **coding_ptrs,
																int block_size,
																const std::vector<int>& data_idxs,
																const std::vector<int>& parity_idxs);
		void decode_with_partial_blocks_(int k_, int *full_matrix,
																		 char **data_ptrs, char **coding_ptrs,
																		 int block_size,
																		 const std::vector<int>& failure_idxs,
																		 const std::vector<int>& parity_idxs);
		void encode_partial_blocks_for_failures_(int k_, int *full_matrix,
																						 char **data_ptrs, char **coding_ptrs,
																						 int block_size,
																						 const std::vector<int>& data_idxs,
																						 const std::vector<int>& parity_idxs,
																						 const std::vector<int>& failure_idxs);
		void encode_partial_blocks_for_failures_v2_(int k_, int *full_matrix,
																						 		char **data_ptrs, char **coding_ptrs,
																						 		int block_size,
																						 		const std::vector<int>& data_idxs,
																								const std::vector<int>& failure_idxs,
																						 		const std::vector<int>& live_idxs);

		// partition stragtegy, subject to single-region fault tolerance
		virtual void partition_flat();
		virtual void partition_random() = 0;
		virtual void partition_optimal() = 0;
		virtual void partition_robust() {};
		void generate_partition();
		std::string print_info(const std::vector<std::vector<int>>& info, const std::string& info_str);

		virtual std::string self_information() = 0;
		virtual std::string type() = 0;

		virtual bool generate_repair_plan(const std::vector<int>& failure_idxs,
																			std::vector<RepairPlan>& plans,
																			bool partial_scheme,
																			bool repair_priority,
																			bool repair_method) = 0;
	};
}
