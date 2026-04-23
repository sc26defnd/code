#include <iostream>
#include <fstream>
#include <vector>
#include <numeric>
#include <algorithm>
#include <map>
#include <unordered_set>
#include <iomanip>
#include <cmath>
#include <string>
#include <random>

using namespace std;


struct ECConfig {
    int num_racks;
    int nodes_per_rack;
    int zones_per_rack; 
    
    //LRC
    int k;
    int l;  
    int g;  
    int chunk_size_mb;
    int num_stripes;

    int group_size() const { return k / l; }
    int total_blocks() const { return k + l + g; }
    int max_blocks_per_rack() const { return g + 1; }
};


struct NodeAddress {
    int rack_id;
    int zone_id;
    int node_id; 
    
    bool operator<(const NodeAddress& other) const { return node_id < other.node_id; }
    bool operator==(const NodeAddress& other) const { return node_id == other.node_id; }
};

struct Block {
    int stripe_id;
    int block_index; 
    int group_id;    // 0 ~ l-1: Local Groups, -1: Global Parity
    bool is_parity;  // true if LP or GP
    string type;     // "Data", "LP", "GP"
    NodeAddress location;
};

struct Stripe {
    int id;
    vector<Block> blocks;
};


class Cluster {
public:
    vector<NodeAddress> nodes;
    int zones_per_rack;
    int nodes_per_rack;
    
    // map[rack_id][zone_id] -> list of node indices in 'nodes' vector
    map<int, map<int, vector<int>>> topology_map;

    Cluster(const ECConfig& cfg) {
        zones_per_rack = cfg.zones_per_rack;
        nodes_per_rack = cfg.nodes_per_rack;
        int cnt = 0;
        for (int r = 0; r < cfg.num_racks; ++r) {
            for (int n = 0; n < cfg.nodes_per_rack; ++n) {
                int z = n % cfg.zones_per_rack;
                nodes.push_back({r, z, cnt});
                topology_map[r][z].push_back(cnt);
                cnt++;
            }
        }
    }
    
    NodeAddress get_node(int rack, int zone, int seq_offset) {
        const auto& candidates = topology_map[rack][zone];
        int idx = seq_offset % candidates.size();
        int global_id = candidates[idx];
        return nodes[global_id];
    }
};


class PlacementManager {
    ECConfig cfg;
    Cluster& cluster;
    std::mt19937 rng; 
    std::uniform_int_distribution<int> dist;
    
public:
    PlacementManager(ECConfig c, Cluster& cl) : cfg(c), cluster(cl), rng(12345), dist(0, c.num_racks - 1) {}

    int count_group_blocks_in_zone(const vector<Block>& blocks, int group_id, int rack, int zone) {
        int count = 0;
        for (const auto& b : blocks) {
            if (b.group_id == group_id && b.location.zone_id == zone) {
                count++;
            }
        }
        return count;
    }

    int count_stripe_blocks_in_zone(const vector<Block>& blocks, int rack, int zone) {
        int count = 0;
        for (const auto& b : blocks) {
            if (b.location.zone_id == zone) {
                count++;
            }
        }
        return count;
    }


    bool is_zone_safe(const Stripe& stripe, int target_zone, int new_group_id, const string& new_type) {
        map<int, int> group_failures;
        int gp_failures = 0;

        for (const auto& b : stripe.blocks) {
            if (b.location.zone_id == target_zone) {
                if (b.type == "GP") gp_failures++;
                else group_failures[b.group_id]++;
            }
        }

        if (new_type == "GP") gp_failures++;
        else group_failures[new_group_id]++;

        int needed_gp = 0;
        for (const auto& kv : group_failures) {
            if (kv.second > 1) needed_gp += (kv.second - 1);
        }

        return needed_gp <= (cfg.g - gp_failures);
    }

    Stripe generate_stripe(int stripe_id) {
        Stripe stripe;
        stripe.id = stripe_id;
        
        int G_PLUS_1 = cfg.g + 1;
        int DATA_PER_GROUP = cfg.group_size();
        int base_rack = dist(rng);
        int current_rack_offset = 0;

        map<int, int> block_rack_map;
        
        for (int g_idx = 0; g_idx < cfg.l; ++g_idx) {
            int start_idx = g_idx * DATA_PER_GROUP;     
            int lp_idx = cfg.k + g_idx;                 
            bool perfect_fit = (DATA_PER_GROUP % G_PLUS_1 == 0);
            
            for (int i = 0; i < DATA_PER_GROUP; ++i) {
                int r_offset = current_rack_offset + (i / G_PLUS_1);
                block_rack_map[start_idx + i] = r_offset;
            }
            
            if (perfect_fit) {
                current_rack_offset += (DATA_PER_GROUP / G_PLUS_1);
            } else {
                int last_data_rack_offset = current_rack_offset + (DATA_PER_GROUP / G_PLUS_1);
                block_rack_map[lp_idx] = last_data_rack_offset;
                current_rack_offset = last_data_rack_offset + 1;
            }
        }

        int parity_rack_offset = current_rack_offset; 
        for (int i = 0; i < cfg.g; ++i) {
            int gp_idx = cfg.k + cfg.l + i;
            block_rack_map[gp_idx] = parity_rack_offset;
        }

        if (DATA_PER_GROUP % G_PLUS_1 == 0) {
             for (int g_idx = 0; g_idx < cfg.l; ++g_idx) {
                 int lp_idx = cfg.k + g_idx;
                 block_rack_map[lp_idx] = parity_rack_offset;
             }
        }
        
        vector<int> processing_order;
        for(int i=0; i<cfg.k; ++i) processing_order.push_back(i);
        for(int i=0; i<cfg.l; ++i) processing_order.push_back(cfg.k + i);
        for(int i=0; i<cfg.g; ++i) processing_order.push_back(cfg.k + cfg.l + i);

        for (int b_idx : processing_order) {
            Block b;
            b.stripe_id = stripe_id;
            b.block_index = b_idx;
            
            if (b_idx < cfg.k) {
                b.type = "Data"; b.group_id = b_idx / DATA_PER_GROUP; b.is_parity = false;
            } else if (b_idx < cfg.k + cfg.l) {
                b.type = "LP"; b.group_id = b_idx - cfg.k; b.is_parity = true;
            } else {
                b.type = "GP"; b.group_id = -1; b.is_parity = true;
            }

            int r_offset = block_rack_map[b_idx];
            int target_rack = (base_rack + r_offset) % cfg.num_racks;
            int selected_zone = -1;

            if (b.type == "Data") {
                selected_zone = (b.block_index) % cfg.zones_per_rack;
            } 
            else if (b.type == "LP") {
                int best_z = -1;
                if (DATA_PER_GROUP <= cfg.zones_per_rack) {
                    int min_group_cnt = 9999, min_stripe_cnt = 9999;
                    for (int z = 0; z < cfg.zones_per_rack; ++z) {
                        if (!is_zone_safe(stripe, z, b.group_id, b.type)) continue; 
                        int g_c = count_group_blocks_in_zone(stripe.blocks, b.group_id, target_rack, z);
                        int s_c = count_stripe_blocks_in_zone(stripe.blocks, target_rack, z);
                        if (g_c < min_group_cnt) { min_group_cnt = g_c; min_stripe_cnt = s_c; best_z = z; }
                        else if (g_c == min_group_cnt && s_c < min_stripe_cnt) { min_stripe_cnt = s_c; best_z = z; }
                    }
                } else {
                    int max_cnt = -1, min_stripe_cnt = 9999;
                    for (int z = 0; z < cfg.zones_per_rack; ++z) {
                        if (!is_zone_safe(stripe, z, b.group_id, b.type)) continue; 
                        int g_c = count_group_blocks_in_zone(stripe.blocks, b.group_id, target_rack, z);
                        int s_c = count_stripe_blocks_in_zone(stripe.blocks, target_rack, z);
                        if (g_c > max_cnt) { max_cnt = g_c; min_stripe_cnt = s_c; best_z = z; } 
                        else if (g_c == max_cnt && s_c < min_stripe_cnt) { min_stripe_cnt = s_c; best_z = z; }
                    }
                }
                
                
                if (best_z == -1) {
                    int min_cnt = 9999;
                    for (int z = 0; z < cfg.zones_per_rack; ++z) {
                        int c = count_stripe_blocks_in_zone(stripe.blocks, target_rack, z);
                        if (c < min_cnt) { min_cnt = c; best_z = z; }
                    }
                }
                selected_zone = best_z;
            } 
            else { 
                int min_cnt = 9999, best_z = 0;
                for (int z = 0; z < cfg.zones_per_rack; ++z) {
                    int c = count_stripe_blocks_in_zone(stripe.blocks, target_rack, z);
                    if (c < min_cnt) { min_cnt = c; best_z = z; }
                }
                selected_zone = best_z;
            }
            
            b.location = cluster.get_node(target_rack, selected_zone, stripe_id + b.block_index);
            stripe.blocks.push_back(b);
        }
        return stripe;
    }

    
    Stripe generate_stripe_dispersed(int stripe_id) {
        Stripe stripe;
        stripe.id = stripe_id;
        
        int DATA_PER_GROUP = cfg.group_size();
        int base_rack = dist(rng); 
        
        map<int, int> block_rack_map;
        for (int g_idx = 0; g_idx < cfg.l; ++g_idx) {
            int start_idx = g_idx * DATA_PER_GROUP;
            for (int i = 0; i < DATA_PER_GROUP; ++i) block_rack_map[start_idx + i] = i; 
        }
        int lp_rack_offset = DATA_PER_GROUP;
        for (int g_idx = 0; g_idx < cfg.l; ++g_idx) block_rack_map[cfg.k + g_idx] = lp_rack_offset;
        
        int gp_rack_offset = DATA_PER_GROUP + 1;
        for (int i = 0; i < cfg.g; ++i) block_rack_map[cfg.k + cfg.l + i] = gp_rack_offset;

        vector<int> processing_order;
        for(int i=0; i < cfg.total_blocks(); ++i) processing_order.push_back(i);

        for (int b_idx : processing_order) {
            Block b;
            b.stripe_id = stripe_id; b.block_index = b_idx;
            if (b_idx < cfg.k) { b.type = "Data"; b.group_id = b_idx / DATA_PER_GROUP; b.is_parity = false; } 
            else if (b_idx < cfg.k + cfg.l) { b.type = "LP"; b.group_id = b_idx - cfg.k; b.is_parity = true; } 
            else { b.type = "GP"; b.group_id = -1; b.is_parity = true; }

            int r_offset = block_rack_map[b_idx];
            int target_rack = (base_rack + r_offset) % cfg.num_racks;
            int selected_zone = b_idx % cfg.zones_per_rack; 

            b.location = cluster.get_node(target_rack, selected_zone, stripe_id + b.block_index);
            stripe.blocks.push_back(b);
        }
        return stripe;
    }
};


class TrafficAnalyzer {
    const ECConfig& cfg;
public:
    TrafficAnalyzer(const ECConfig& c) : cfg(c) {}

    pair<double, bool> calculate_single_block_repair_traffic(const Stripe& stripe, const Block& target_b, const unordered_set<int>& failed_node_ids) {
        if (failed_node_ids.find(target_b.location.node_id) == failed_node_ids.end()) {
            return {0.0, true};
        }

        double traffic_mb = 0;
        int target_rack = target_b.location.rack_id; 
        
        
        if (target_b.type != "GP") {
            int lost_in_group = 0;
            vector<Block> group_survivors;
            
            for (const auto& b : stripe.blocks) {
                if (b.group_id == target_b.group_id) {
                    if (failed_node_ids.count(b.location.node_id)) {
                        lost_in_group++;
                    } else {
                        group_survivors.push_back(b);
                    }
                }
            }
            
            if (lost_in_group == 1) {
                int needed = cfg.group_size(); 
                if (group_survivors.size() < needed) return {0.0, false}; 
                
                int local_hits = 0;
                for (const auto& b : group_survivors) {
                    if (b.location.rack_id == target_rack) local_hits++;
                }
                
                int take_remote = needed - min(local_hits, needed);
                traffic_mb += take_remote * cfg.chunk_size_mb; 
                return {traffic_mb, true};
            }
        }
        
        
        map<int, int> global_surviving_data;
        map<int, int> global_surviving_lp;
        int global_surviving_gp = 0;

        map<int, int> local_surviving_data;
        map<int, int> local_surviving_lp;
        int local_surviving_gp = 0;
        
        for (const auto& b : stripe.blocks) {
            if (failed_node_ids.count(b.location.node_id)) continue;
            
            
            if (b.type == "Data") global_surviving_data[b.group_id]++;
            else if (b.type == "LP") global_surviving_lp[b.group_id]++;
            else if (b.type == "GP") global_surviving_gp++;

            
            if (b.location.rack_id == target_rack) {
                if (b.type == "Data") local_surviving_data[b.group_id]++;
                else if (b.type == "LP") local_surviving_lp[b.group_id]++;
                else if (b.type == "GP") local_surviving_gp++;
            }
        }

        
        int global_rank = global_surviving_gp; 
        for (int g_idx = 0; g_idx < cfg.l; ++g_idx) {
            global_rank += min(global_surviving_data[g_idx] + global_surviving_lp[g_idx], cfg.group_size());
        }

        
        if (global_rank < cfg.k) return {0.0, false}; 

        
        int local_hits = local_surviving_gp;
        for (int g_idx = 0; g_idx < cfg.l; ++g_idx) {
            local_hits += min(local_surviving_data[g_idx] + local_surviving_lp[g_idx], cfg.group_size());
        }
        
        int needed = cfg.k;
        int take_remote = needed - min(local_hits, needed);
        traffic_mb += take_remote * cfg.chunk_size_mb;
        return {traffic_mb, true};
    }
};


struct ExperimentResult {
    double total_rack_tb = 0.0;
    double total_zone_tb = 0.0;
    double total_rack_disp_tb = 0.0;
    
    double coupled_avg_block_single_gb = 0.0; 
    double coupled_avg_block_rack_gb = 0.0;
    double coupled_avg_block_zone_gb = 0.0;
    
    double dispersed_avg_block_single_gb = 0.0; 
    double dispersed_avg_block_rack_gb = 0.0;
    double dispersed_avg_block_zone_gb = 0.0;

    
    long long coupled_unrep_single_stripes = 0;  
    long long coupled_unrep_rack_stripes = 0;
    long long coupled_unrep_zone_stripes = 0;
    
    long long dispersed_unrep_single_stripes = 0; 
    long long dispersed_unrep_rack_stripes = 0;
    long long dispersed_unrep_zone_stripes = 0;
};

ExperimentResult run_experiment(ECConfig cfg, int exp_index) {
    Cluster cluster(cfg);
    PlacementManager placer(cfg, cluster);
    TrafficAnalyzer analyzer(cfg);
    
    int NUM_STRIPES = cfg.num_stripes;
    vector<Stripe> stripes_coupled;
    vector<Stripe> stripes_dispersed;
    stripes_coupled.reserve(NUM_STRIPES);
    stripes_dispersed.reserve(NUM_STRIPES);
    
    for (int i = 0; i < NUM_STRIPES; ++i) {
        stripes_coupled.push_back(placer.generate_stripe(i));
        stripes_dispersed.push_back(placer.generate_stripe_dispersed(i));
    }
    
    ExperimentResult res;


    long long rep_cnt_coupled_single = 0, rep_cnt_coupled_rack = 0, rep_cnt_coupled_zone = 0;
    double tot_traffic_coupled_single = 0.0, tot_traffic_coupled_rack = 0.0, tot_traffic_coupled_zone = 0.0;

    for (const auto& s : stripes_coupled) {

        bool stripe_unrep_single = false;
        bool stripe_unrep_rack = false;
        bool stripe_unrep_zone = false;

        for (const auto& b : s.blocks) {
            int target_rack = b.location.rack_id;
            int target_zone = b.location.zone_id;

            // 1. Single Block Down
            unordered_set<int> failed_nodes_single = {b.location.node_id};
            auto res_single = analyzer.calculate_single_block_repair_traffic(s, b, failed_nodes_single);
            if (res_single.second) { tot_traffic_coupled_single += res_single.first; rep_cnt_coupled_single++; } 
            else stripe_unrep_single = true;
            // 2. Rack Down
            unordered_set<int> failed_nodes_rack;
            for (const auto& kv : cluster.topology_map[target_rack]) {
                for(int nid : kv.second) failed_nodes_rack.insert(nid);
            }
            auto res_rack = analyzer.calculate_single_block_repair_traffic(s, b, failed_nodes_rack);
            if (res_rack.second) { tot_traffic_coupled_rack += res_rack.first; rep_cnt_coupled_rack++; } 
            else stripe_unrep_rack = true;

            // 3. Zone Down
            unordered_set<int> failed_nodes_zone;
            for (int r = 0; r < cfg.num_racks; ++r) {
                if (cluster.topology_map.count(r) && cluster.topology_map[r].count(target_zone)) {
                    for (int nid : cluster.topology_map[r][target_zone]) failed_nodes_zone.insert(nid);
                }
            }
            auto res_zone = analyzer.calculate_single_block_repair_traffic(s, b, failed_nodes_zone);
            if (res_zone.second) { tot_traffic_coupled_zone += res_zone.first; rep_cnt_coupled_zone++; } 
            else stripe_unrep_zone = true;
        }
        if (stripe_unrep_single) res.coupled_unrep_single_stripes++;
        if (stripe_unrep_rack) res.coupled_unrep_rack_stripes++;
        if (stripe_unrep_zone) res.coupled_unrep_zone_stripes++;
    }
    res.total_rack_tb = tot_traffic_coupled_rack / (1024.0 * 1024.0);
    res.total_zone_tb = tot_traffic_coupled_zone / (1024.0 * 1024.0);
    res.coupled_avg_block_single_gb = rep_cnt_coupled_single > 0 ? (tot_traffic_coupled_single / rep_cnt_coupled_single / 1024.0) : 0.0;
    res.coupled_avg_block_rack_gb = rep_cnt_coupled_rack > 0 ? (tot_traffic_coupled_rack / rep_cnt_coupled_rack / 1024.0) : 0.0;
    res.coupled_avg_block_zone_gb = rep_cnt_coupled_zone > 0 ? (tot_traffic_coupled_zone / rep_cnt_coupled_zone / 1024.0) : 0.0;


    long long rep_cnt_disp_single = 0, rep_cnt_disp_rack = 0, rep_cnt_disp_zone = 0;
    double tot_traffic_disp_single = 0.0, tot_traffic_disp_rack = 0.0, tot_traffic_disp_zone = 0.0;

    for (const auto& s : stripes_dispersed) {
        
        bool stripe_unrep_single_disp = false;
        bool stripe_unrep_rack_disp = false;
        bool stripe_unrep_zone_disp = false;

        for (const auto& b : s.blocks) {
            int target_rack = b.location.rack_id;
            int target_zone = b.location.zone_id;

            // 1. Single Block Down
            unordered_set<int> failed_nodes_single = {b.location.node_id};
            auto res_single = analyzer.calculate_single_block_repair_traffic(s, b, failed_nodes_single);
            if (res_single.second) { tot_traffic_disp_single += res_single.first; rep_cnt_disp_single++; } 
            else stripe_unrep_single_disp = true;

            // 2. Rack Down
            unordered_set<int> failed_nodes_rack;
            for (const auto& kv : cluster.topology_map[target_rack]) {
                for(int nid : kv.second) failed_nodes_rack.insert(nid);
            }
            auto res_rack = analyzer.calculate_single_block_repair_traffic(s, b, failed_nodes_rack);
            if (res_rack.second) { tot_traffic_disp_rack += res_rack.first; rep_cnt_disp_rack++; } 
            else stripe_unrep_rack_disp = true;

            // 3. Zone Down
            unordered_set<int> failed_nodes_zone;
            for (int r = 0; r < cfg.num_racks; ++r) {
                if (cluster.topology_map.count(r) && cluster.topology_map[r].count(target_zone)) {
                    for (int nid : cluster.topology_map[r][target_zone]) failed_nodes_zone.insert(nid);
                }
            }
            auto res_zone = analyzer.calculate_single_block_repair_traffic(s, b, failed_nodes_zone);
            if (res_zone.second) { tot_traffic_disp_zone += res_zone.first; rep_cnt_disp_zone++; } 
            else stripe_unrep_zone_disp = true;
        }
        if (stripe_unrep_single_disp) res.dispersed_unrep_single_stripes++;
        if (stripe_unrep_rack_disp) res.dispersed_unrep_rack_stripes++;
        if (stripe_unrep_zone_disp) res.dispersed_unrep_zone_stripes++;
    }

    res.total_rack_disp_tb = tot_traffic_disp_rack / (1024.0 * 1024.0);
    res.dispersed_avg_block_single_gb = rep_cnt_disp_single > 0 ? (tot_traffic_disp_single / rep_cnt_disp_single / 1024.0) : 0.0;
    res.dispersed_avg_block_rack_gb = rep_cnt_disp_rack > 0 ? (tot_traffic_disp_rack / rep_cnt_disp_rack / 1024.0) : 0.0;
    res.dispersed_avg_block_zone_gb = rep_cnt_disp_zone > 0 ? (tot_traffic_disp_zone / rep_cnt_disp_zone / 1024.0) : 0.0;

    return res;
}

int main() {
    ifstream infile("config.txt");
    if (!infile.is_open()) {
        cerr << "Error: Could not open config.txt" << endl;
        return 1;
    }

    ofstream outfile("result/results_exp_test.csv");
    if (!outfile.is_open()) {
        cerr << "Error: Could not create result/results_exp.csv" << endl;
        return 1;
    }

    outfile << "Exp_ID,Racks,NodesPerRack,ZonesPerRack,K,L,G,ChunkSize,NumStripes,"
            << "C_Toal_Rack_TB,C_Total_Zone_TB,D_Total_Rack_TB,"
            << "C_AvgBlk_Single_GB,C_AvgBlk_Rack_GB,C_AvgBlk_Zone_GB,"
            << "D_AvgBlk_Single_GB,D_AvgBlk_Rack_GB,D_AvgBlk_Zone_GB,"
            << "C_Unrep_Single,C_Unrep_Rack,C_Unrep_Zone,"
            << "D_Unrep_Single,D_Unrep_Rack,D_Unrep_Zone" 
            << endl;

    int exp_cnt = 1;
    int racks, nodes, zones, k, l, g, chunk, stripes;

    while (infile >> racks >> nodes >> zones >> k >> l >> g >> chunk >> stripes) {
        ECConfig cfg;
        cfg.num_racks = racks;
        cfg.nodes_per_rack = nodes;
        cfg.zones_per_rack = zones;
        cfg.k = k;
        cfg.l = l;
        cfg.g = g;
        cfg.chunk_size_mb = chunk;
        cfg.num_stripes = stripes; 

        ExperimentResult res = run_experiment(cfg, exp_cnt);

        outfile << exp_cnt << ","
                << cfg.num_racks << ","
                << cfg.nodes_per_rack << ","
                << cfg.zones_per_rack << ","
                << cfg.k << ","
                << cfg.l << ","
                << cfg.g << ","
                << cfg.chunk_size_mb << ","
                << cfg.num_stripes << ","
                << fixed << setprecision(6)
                //total
                << res.total_rack_tb << ","
                << res.total_zone_tb << ","
                << res.total_rack_disp_tb << ","
                // Coupled
                << res.coupled_avg_block_single_gb << ","
                << res.coupled_avg_block_rack_gb << ","
                << res.coupled_avg_block_zone_gb << ","
                // Dispersed
                << res.dispersed_avg_block_single_gb << ","
                << res.dispersed_avg_block_rack_gb << ","
                << res.dispersed_avg_block_zone_gb << ","
                // Unrep Counters
                << res.coupled_unrep_single_stripes << ","
                << res.coupled_unrep_rack_stripes << ","
                << res.coupled_unrep_zone_stripes << ","
                << res.dispersed_unrep_single_stripes << ","
                << res.dispersed_unrep_rack_stripes << ","
                << res.dispersed_unrep_zone_stripes << endl;
                
        exp_cnt++;
    }

    cout << "\nAll experiments finished. Results saved to 'result/results_exp.csv'." << endl;
    infile.close();
    outfile.close();

    return 0;
}