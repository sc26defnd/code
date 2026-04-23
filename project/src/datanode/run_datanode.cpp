#include "datanode.h"

using namespace ECProject;
int main(int argc, char **argv)
{
  pid_t pid = fork();
  if (pid > 0) {
    exit(0);
  }
  setsid();
  if (true) {
    umask(0);
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
  }

  std::string ip(argv[1]);
  int port = std::stoi(argv[2]);
  std::string logfile = "";
  if (argc == 4) {
    logfile = argv[3];
  }
  Datanode datanode(ip, port, logfile);
  datanode.run();
  return 0;
}