#include "coordinator.h"

using namespace ECProject;
int main(int argc, char **argv)
{
  if (IF_LOG_TO_FILE) {
    umask(0);
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
  }
  
  char buff[256];
  getcwd(buff, 256);
  std::string cwf = std::string(argv[0]);
  std::string xml_path = std::string(buff) +
      cwf.substr(1, cwf.rfind('/') - 1) + "/../clusterinfo.xml";
  Coordinator coordinator("0.0.0.0", COORDINATOR_PORT, xml_path);
  if (argc == 2) {
    std::string config_file = std::string(buff) +
        cwf.substr(1, cwf.rfind('/') - 1) + "/../" + std::string(argv[1]);
    coordinator.init_ec_schema(config_file);
  }
  coordinator.run();
  return 0;
}