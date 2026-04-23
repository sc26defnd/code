# DEFND: Decoupling Failure and Update Domains for Efficient Rack-Scale Storage

This repository contains the source code and experimental scripts for the paper **"DEFND: Decoupling Failure and Update Domains for Efficient Rack-Scale Storage"**.



## 🛠️ Prerequisites

### Hardware Requirements
* **OS:** Linux (Ubuntu 22.04)
* **Disk:** Sufficient space for generating dummy data blocks.

### Software Dependencies

* 𝐶𝑀𝑎𝑘𝑒 3.22.0. Along with required dependencies.
* 𝐺𝐶𝐶 11.1.0. Along with required dependencies.
* 𝑔𝑓-𝑐𝑜𝑚𝑝𝑙𝑒𝑡𝑒. https://github.com/ceph/gf-complete.git
* 𝑗𝑒𝑟𝑎𝑠𝑢𝑟𝑒. https://github.com/tsuraan/Jerasure.git
* 𝑦𝑎𝑙𝑎𝑛𝑡𝑖𝑛𝑔𝑙𝑖𝑏𝑠. https://github.com/alibaba/yalantinglibs.git
* 𝑀𝑒𝑚𝑐𝑎𝑐ℎ𝑒𝑑(𝑜𝑝𝑡𝑖𝑜𝑛𝑎𝑙). Used for in-memory data storage. (𝑀𝑒𝑚𝑐𝑎𝑐ℎ𝑒𝑑 1.6.31 and 𝑙𝑖𝑏𝑚𝑒𝑚𝑐𝑎𝑐ℎ𝑒𝑑 1.0.18.)
* 𝑅𝑒𝑑𝑖𝑠(𝑜𝑝𝑡𝑖𝑜𝑛𝑎𝑙). Used for in-memory data storage. (Latest versions of ℎ𝑖𝑟𝑒𝑑𝑖𝑠, 𝑟𝑒𝑑𝑖𝑠-𝑝𝑙𝑢𝑠-𝑝𝑙𝑢𝑠 and 𝑟𝑒𝑑𝑖𝑠 from GitHub.)


## 🚀 Build & Installation

### Install Dependencies

We provide a script to automatically install and configure the required third-party libraries.

```bash
bash install_third_party.sh
```

### Compile the Project

```bash
cd project
bash compile.sh
```

## ⚙️ Configuration

After the deployment scheme is determined, configure the component information of the storage system in 𝑔𝑒𝑛𝑒𝑟𝑎𝑡𝑜𝑟_𝑠ℎ.𝑝𝑦 under the 𝑡𝑜𝑜𝑙𝑠/ directory.

* The number of racks.
* The numberof data-nodes in each rack.
* The address of the proxy.
* The network core bandwidth limit (e.g., 2,500,000) according to your setup.

Run the script to generate the necessary files for each physical node, and launch the data nodes, proxies, and coordinator.

```bash
py generator_sh.py
bash exp.sh 2
```


## 📊 Run

Configure the erasure coding scheme in 𝑐𝑜𝑛𝑓𝑖𝑔.𝑖𝑛𝑖 under the 𝑝𝑟𝑜𝑗𝑒𝑐𝑡/ directory

* Modify prototype/project/config.ini to adjust experiment settings:
    * if_zone_aware: Zone aware or not.

    * ec_type: Erasure code type (e.g., AZURE_LRC).

    * placement_rule: Placement rule of a single stripe (e.g., OPTIMAL, ROBUST).

    * block_size: 4MB is default.

    * zones_per_rack: The number of zones.

    * read_mode: Read mode (e.g., normal, single block, rack maintenance, zone maintenance)

    * ec_schema: Erasure coding parameters (e.g., k, l, g for LRC).

Generate input data files.
```bash
cd tools
py generator_file.py [num] [size] [unit]
py generator_trace.py
```


Run in a local node for test

```bash
bash run_proxy_datanode.sh
./project/build/run_coordinator
./project/build/run_client config.ini [stripe_num] [eva_num]
```

Run in multiple nodes for testbed experiments

* Run the coordinator, all data nodes, and proxies. 

```bash
bash run_server.sh
```

* Launch the client and complete the evaluation.
```bash
bash run_client.sh
```
Depending on target metrics, modify run_client.sh to specify evaluation settings, such as the number of stripes and the evaluation mode (e.g., single-block repair, rack maintenance, or zone maintenance).
