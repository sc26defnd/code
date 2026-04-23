import os

current_path = os.getcwd()
parent_path = os.path.dirname(current_path)
cluster_number = 8
datanode_number_per_cluster = 20
datanode_port_start = 17600
cluster_id_start = 0
iftest = False
vc_per_pc = 1
ifredis = False
ifmemcached = False

proxy_ip_list = [
    ["192.168.0.219",32406],
    ["192.168.0.220",32406],
    ["192.168.0.221",32406],
    ["192.168.0.222",32406],
    ["192.168.0.223",32406],
    ["192.168.0.224",32406],
    ["192.168.0.225",32406],
    ["192.168.0.226",32406],
]
networkcore_address = "192.168.0.227:17550"
networkcore_ip = "192.168.0.227"
networkcore_port = 17550

if iftest:
    proxy_ip_list = [
        ["0.0.0.0",50005],
        ["0.0.0.0",50035],
        ["0.0.0.0",50065],
        ["0.0.0.0",50095],
        ["0.0.0.0",50125],
        ["0.0.0.0",50155],
        ["0.0.0.0",50185],
        ["0.0.0.0",50215],
        ["0.0.0.0",50245],
        ["0.0.0.0",50275],
        ["0.0.0.0",50305],
        ["0.0.0.0",50335],
        ["0.0.0.0",50365],
        ["0.0.0.0",50395],
        ["0.0.0.0",50425],
        ["0.0.0.0",50455],
        ["0.0.0.0",50485],
        ["0.0.0.0",50515],
        ["0.0.0.0",50545],
        ["0.0.0.0",50575],
        ["0.0.0.0",50605],
        ["0.0.0.0",50635],
        ["0.0.0.0",50665],
        ["0.0.0.0",50695],
        ["0.0.0.0",50725],
        ["0.0.0.0",50755],
    ]
    networkcore_address = "0.0.0.0:17590"
    networkcore_ip = "0.0.0.0"
    networkcore_port = 17590

proxy_num = len(proxy_ip_list)

cluster_informtion = {}
def generate_cluster_info_dict():
    proxy_cnt = 0
    proxy_idx = 0
    for i in range(cluster_number):
        new_cluster = {}
        new_cluster["proxy"] = proxy_ip_list[proxy_idx][0]+":"+str(proxy_ip_list[proxy_idx][1] + 7 * proxy_cnt)
        datanode_list = []
        for j in range(datanode_number_per_cluster):
            port = datanode_port_start + i * datanode_number_per_cluster+ j
            datanode_list.append([proxy_ip_list[proxy_idx][0], port])
        new_cluster["datanode"] = datanode_list
        cluster_informtion[i] = new_cluster
        proxy_cnt += 1
        if proxy_cnt == vc_per_pc:
            proxy_idx += 1
            proxy_cnt = 0            
            
def generate_run_proxy_datanode_file():
    file_name = parent_path + '/run_proxy_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        for cluster_id in cluster_informtion.keys():
            print("cluster_id",cluster_id)
            for each_datanode in cluster_informtion[cluster_id]["datanode"]:
                f.write("./project/build/run_datanode "+str(each_datanode[0])+" "+str(each_datanode[1])+"&\n")
            f.write("\n") 
        f.write("./project/build/run_datanode "+networkcore_ip+" "+str(networkcore_port)+"&\n")
        f.write("\n")
        for i in range(cluster_number):
            proxy_ip_port = proxy_ip_list[i]
            f.write("./project/build/run_proxy "+str(proxy_ip_port[0])+" "+str(proxy_ip_port[1])+" "+networkcore_address+"&\n")   
        f.write("\n")
        f.write("sleep 3\n")
        f.write("echo 'All Datanodes and Proxies started.'\n")

def generate_run_redis_file(bias=1000):
    file_name = parent_path + '/run_server.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 redis-server\n")
        f.write("sudo sysctl vm.overcommit_memory=1\n")
        f.write("\n")
        for cluster_id in cluster_informtion.keys():
            print("cluster_id",cluster_id)
            for each_datanode in cluster_informtion[cluster_id]["datanode"]:
                f.write("./project/third_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port "+str(each_datanode[1] + bias)+"\n")
            f.write("\n") 
        f.write("./project/third_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port "+str(networkcore_port + 1000)+"\n")
        f.write("\n")
        
def generate_run_memcached_file(bias=1000):
    file_name = parent_path + '/run_server.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 memcached\n")
        f.write("\n")
        for cluster_id in cluster_informtion.keys():
            print("cluster_id",cluster_id)
            for each_datanode in cluster_informtion[cluster_id]["datanode"]:
                f.write("./project/third_party/memcached/bin/memcached -m 128 -p " + str(each_datanode[1] + bias) + " --max-item-size=4M -vv -d\n")
            f.write("\n") 
        f.write("./project/third_party/memcached/bin/memcached -m 128 -p " + str(networkcore_port + bias) + " --max-item-size=4M -vv -d\n")
        f.write("\n")

def generater_cluster_information_xml():
    file_name = parent_path + '/project/clusterinfo.xml'
    import xml.etree.ElementTree as ET
    root = ET.Element('clusters')
    root.text = "\n\t"
    for cluster_id in cluster_informtion.keys():
        cluster = ET.SubElement(root, 'cluster', {'id': str(cluster_id), 'proxy': cluster_informtion[cluster_id]["proxy"]})
        cluster.text = "\n\t\t"
        datanodes = ET.SubElement(cluster, 'datanodes')
        datanodes.text = "\n\t\t\t"
        for index,each_datanode in enumerate(cluster_informtion[cluster_id]["datanode"]):
            datanode = ET.SubElement(datanodes, 'datanode', {'uri': str(each_datanode[0])+":"+str(each_datanode[1])})
            #datanode.text = '\n\t\t\t'
            if index == len(cluster_informtion[cluster_id]["datanode"]) - 1:
                datanode.tail = '\n\t\t'
            else:
                datanode.tail = '\n\t\t\t'
        datanodes.tail = '\n\t'
        if cluster_id == len(cluster_informtion)-1:
            cluster.tail = '\n'
        else:
            cluster.tail = '\n\t'
    #root.tail = '\n'
    tree = ET.ElementTree(root)
    tree.write(file_name, encoding="utf-8", xml_declaration=True)

def cluster_generate_run_redis_datanode_file(i, bias=1000):
    if not os.path.exists(parent_path + '/run_cluster_sh/' + str(i)):
        os.makedirs(parent_path + '/run_cluster_sh/' + str(i))
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 redis-server\n")
        f.write("pkill -9 run_datanode\n")
        f.write("sudo sysctl vm.overcommit_memory=1\n")
        f.write("\n")
        for j in range(i * vc_per_pc, (i + 1) * vc_per_pc):
            for each_datanode in cluster_informtion[j]["datanode"]:
                f.write("./project/third_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port "+str(each_datanode[1] + bias)+"\n")
            f.write("\n") 
        for j in range(i * vc_per_pc, (i + 1) * vc_per_pc):
            for each_datanode in cluster_informtion[j]["datanode"]:
                f.write("./project/build/run_datanode "+str(each_datanode[0])+" "+str(each_datanode[1])+"\n")
            f.write("\n") 

def cluster_generate_run_memcached_datanode_file(i, bias=1000):
    if not os.path.exists(parent_path + '/run_cluster_sh/' + str(i)):
        os.makedirs(parent_path + '/run_cluster_sh/' + str(i))
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 memcached\n")
        f.write("pkill -9 run_datanode\n")
        f.write("\n")
        for j in range(i * vc_per_pc, (i + 1) * vc_per_pc):
            for each_datanode in cluster_informtion[j]["datanode"]:
                f.write("./project/third_party/memcached/bin/memcached -m 128 -p " + str(each_datanode[1] + bias) + " --max-item-size=4M -vv -d\n")
            f.write("\n") 
        for j in range(i * vc_per_pc, (i + 1) * vc_per_pc):
            for each_datanode in cluster_informtion[j]["datanode"]:
                f.write("./project/build/run_datanode "+str(each_datanode[0])+" "+str(each_datanode[1])+"\n")
            f.write("\n") 

def cluster_generate_run_datanode_file(i, bias=1000):
    if not os.path.exists(parent_path + '/run_cluster_sh/' + str(i)):
        os.makedirs(parent_path + '/run_cluster_sh/' + str(i))
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("\n")
        for j in range(i * vc_per_pc, (i + 1) * vc_per_pc):
            for each_datanode in cluster_informtion[j]["datanode"]:
                f.write("./project/build/run_datanode "+str(each_datanode[0])+" "+str(each_datanode[1])+"\n")
            f.write("\n") 

def cluster_generate_run_proxy_file(i):
    if not os.path.exists(parent_path + '/run_cluster_sh/' + str(i)):
        os.makedirs(parent_path + '/run_cluster_sh/' + str(i))
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_proxy.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        ip = proxy_ip_list[i][0]
        port = proxy_ip_list[i][1]
        for j in range(i * vc_per_pc, (i + 1) * vc_per_pc):
            #f.write("./project/build/run_proxy "+ip+" "+str(port)+" "+networkcore_address+"&\n")   
            cmd = f"nohup ./project/build/run_proxy {ip} {port} {networkcore_address} > /dev/null 2>&1 &\n"
            f.write(cmd)
            f.write("\n")
            port += 7

def cluster_generate_run_datanode_file(i):
    if not os.path.exists(parent_path + '/run_cluster_sh/' + str(i)):
        os.makedirs(parent_path + '/run_cluster_sh/' + str(i))
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("\n")
        for j in range(i * vc_per_pc, (i + 1) * vc_per_pc):
            for each_datanode in cluster_informtion[j]["datanode"]:
                #f.write("./project/build/run_datanode "+str(each_datanode[0])+" "+str(each_datanode[1])+"&\n")
                cmd = f"nohup ./project/build/run_datanode {each_datanode[0]} {each_datanode[1]} > /dev/null 2>&1 &\n"
                f.write(cmd)
            f.write("\n") 

def cluster_generate_run_redis_file(i, bias=1000):
    if not os.path.exists(parent_path + '/run_cluster_sh/' + str(i)):
        os.makedirs(parent_path + '/run_cluster_sh/' + str(i))
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 redis-server\n")
        f.write("sudo sysctl vm.overcommit_memory=1\n")
        f.write("\n")
        for j in range(i * vc_per_pc, (i + 1) * vc_per_pc):
            for each_datanode in cluster_informtion[j]["datanode"]:
                f.write("./project/third_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port "+str(each_datanode[1] + bias)+"\n")
            f.write("\n")

def cluster_generate_run_memcached_file(i, bias=1000):
    if not os.path.exists(parent_path + '/run_cluster_sh/' + str(i)):
        os.makedirs(parent_path + '/run_cluster_sh/' + str(i))
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 memcached\n")
        f.write("sudo sysctl vm.overcommit_memory=1\n")
        f.write("\n")
        for j in range(i * vc_per_pc, (i + 1) * vc_per_pc):
            for each_datanode in cluster_informtion[j]["datanode"]:
                f.write("./project/third_party/memcached/bin/memcached -m 128 -p " + str(each_datanode[1] + bias) + " --max-item-size=4M -vv -d\n")
            f.write("\n")

def cluster_generate_sh_for_networkcore_redis(i, bias=1000):
    if not os.path.exists(parent_path + '/run_cluster_sh/' + str(i)):
        os.makedirs(parent_path + '/run_cluster_sh/' + str(i))
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 redis-server\n")
        f.write("sudo sysctl vm.overcommit_memory=1\n")
        f.write("\n")
        f.write("./project/third_party/redis/bin/redis-server --daemonize yes --bind 0.0.0.0 --port "+str(networkcore_port + bias)+"\n")
        f.write("\n")
        f.write("./project/build/run_datanode "+"0.0.0.0"+" "+str(networkcore_port)+"\n")
        f.write("\n")
        
def cluster_generate_sh_for_networkcore_memcached(i, bias=1000):
    if not os.path.exists(parent_path + '/run_cluster_sh/' + str(i)):
        os.makedirs(parent_path + '/run_cluster_sh/' + str(i))
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 memcached\n")
        f.write("\n")
        f.write("./project/third_party/memcached/bin/memcached -m 128 -p " + str(networkcore_port + bias) + " --max-item-size=4M -vv -d\n")
        f.write("\n")
        f.write("./project/build/run_datanode "+"0.0.0.0"+" "+str(networkcore_port)+"\n")
        f.write("\n")

def cluster_generate_sh_for_networkcore(i):
    if not os.path.exists(parent_path + '/run_cluster_sh/' + str(i)):
        os.makedirs(parent_path + '/run_cluster_sh/' + str(i))
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("\n")
        #f.write("./project/build/run_datanode "+"0.0.0.0"+" "+str(networkcore_port)+"&\n")
        cmd = f"nohup ./project/build/run_datanode 192.168.0.227 {networkcore_port} > /dev/null 2>&1 &\n"
        f.write(cmd)
        f.write("\n")

if __name__ == "__main__":
    generate_cluster_info_dict()
    generater_cluster_information_xml()
    if iftest:
        generate_run_proxy_datanode_file()
        if ifredis:
            generate_run_redis_file()
        elif ifmemcached:
            generate_run_memcached_file()
    else:
        if not os.path.exists(parent_path + '/run_cluster_sh'):
            os.makedirs(parent_path + '/run_cluster_sh')
        cnt = 0
        for i in range(cluster_number // vc_per_pc):
            cluster_generate_run_proxy_file(i)
            if ifredis:
                cluster_generate_run_redis_datanode_file(i)
            elif ifmemcached:
                cluster_generate_run_memcached_datanode_file(i)
            else:
                cluster_generate_run_datanode_file(i)
            cnt += 1
        if ifredis:
            cluster_generate_sh_for_networkcore_redis(cnt)
        elif ifmemcached:
            cluster_generate_sh_for_networkcore_memcached(cnt)
        else:
            cluster_generate_sh_for_networkcore(cnt)
        cnt += 1
    