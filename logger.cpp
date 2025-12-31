#include <fstream>
#include <string>
#include <mutex>

class AofLogger {
private:
    std::ofstream file_;
    std::mutex mutex_;
    std::string filename_;

public:
    AofLogger(const std::string& filename) : filename_(filename) {
        file_.open(filename, std::ios::app);
    }

    void append(const std::string& command) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (file_.is_open()) {
            file_ << command << "\n";
            file_.flush();
        }
    }

    void clearFile() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (file_.is_open()) file_.close();
        file_.open(filename_, std::ios::out | std::ios::trunc);
    }

    ~AofLogger() {
        if (file_.is_open()) file_.close();
    }
};