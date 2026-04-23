#include <iostream>
#include <fstream>
#include <string>
#include <ctime>

namespace ECProject
{
  class Logger
  {
  public:
    enum LogLevel
    {
      INFO,
      WARNING,
      ERROR,
      DEBUG
    };

    Logger(const std::string &filename)
    {
      logFile.open(filename, std::ios::app); // append mode
      if (!logFile.is_open())
      {
        std::cerr << "Failed to open log file: " << filename << std::endl;
      }
    }

    ~Logger()
    {
      if (logFile.is_open())
      {
        logFile.close();
      }
    }

    void log(LogLevel level, const std::string &message)
    {
      if (!logFile.is_open())
        return;

      if (level == DEBUG)
      {
        logFile << message;
      }
      else
      {
        std::string levelStr = getLevelString(level);
        std::string timeStr = getCurrentTime();
        logFile << "[" << timeStr << "] [" << levelStr << "] " << message;
      }
      logFile.flush();
    }

  private:
    std::ofstream logFile;

    std::string getLevelString(LogLevel level)
    {
      switch (level)
      {
      case INFO:
        return "INFO";
      case WARNING:
        return "WARNING";
      case ERROR:
        return "ERROR";
      case DEBUG:
        return "DEBUG";
      default:
        return "UNKNOWN";
      }
    }

    std::string getCurrentTime()
    {
      std::time_t now = std::time(nullptr);
      char buf[20];
      std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
      return std::string(buf);
    }
  };
}