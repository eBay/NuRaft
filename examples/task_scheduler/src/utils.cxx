#include "../include/utils.hxx"
#include "../include/task.hxx"
#include <cstring>
#include <iomanip>
#include <mutex>
#include <random>
#include <sstream>

auto get_current_time() -> double {
  return std::chrono::duration<double>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int heap_parent(int index) {
  return (index - 1) / 2;
}

int heap_left_child(int index) {
  return 2 * index + 1;
}

int heap_right_child(int index) {
  return 2 * index + 2;
}

auto bytes_to_hex_string(const std::array<uint8_t, 16>& bytes,
                        const std::array<size_t, 4>& dash_positions)
                        -> std::string {
  std::ostringstream oss;
  for (size_t i = 0; i < bytes.size(); ++i) {
    oss << std::hex << std::setw(2) << std::setfill('0')
        << static_cast<int>(bytes[i]);
    for (size_t dash_pos : dash_positions) {
      if (i == dash_pos) {
        oss << "-";
        break;
      }
    }
  }
  return oss.str();
}

auto generate_random_bytes(size_t count) -> std::vector<uint8_t> {
  static thread_local std::mt19937_64 rng(std::random_device{}());
  std::uniform_int_distribution<uint64_t> dist;

  std::vector<uint8_t> bytes(count);

  for (size_t i = 0; i < count; i += 8) {
    uint64_t random_value = dist(rng);
    size_t bytes_to_copy = std::min(size_t(8), count - i);
    std::memcpy(bytes.data() + i, &random_value, bytes_to_copy);
  }

  return bytes;
}

auto generate_uuid_v4() -> std::array<uint8_t, 16> {
  std::array<uint8_t, 16> uuid_bytes;
  auto random_bytes = generate_random_bytes(16);
  std::copy(random_bytes.begin(), random_bytes.end(), uuid_bytes.begin());

  // UUIDv4 format:
  // version = 4 (random)
  uuid_bytes[6] = (uuid_bytes[6] & 0x0F) | 0x40;

  // variant = RFC 4122 (10xxxxxx)
  uuid_bytes[8] = (uuid_bytes[8] & 0x3F) | 0x80;

  return uuid_bytes;
}

// CircularBuffer template implementations
template<typename T>
CircularBuffer<T>::CircularBuffer(size_t capacity)
    : capacity_(capacity), head_(0), tail_(0), size_(0) {
  buffer_.resize(capacity_);
}

template<typename T>
bool CircularBuffer<T>::push(const T& item) {
  std::unique_lock<std::mutex> lock(mutex_);

  buffer_[tail_] = item;
  tail_ = (tail_ + 1) % capacity_;

  if (size_ < capacity_) {
    size_++;
  } else {
    // Buffer full, overwrite oldest
    head_ = (head_ + 1) % capacity_;
  }

  return true;
}

template<typename T>
optional<T> CircularBuffer<T>::pop() {
  std::unique_lock<std::mutex> lock(mutex_);

  if (size_ == 0) {
    return optional<T>();
  }

  T item = buffer_[head_];
  head_ = (head_ + 1) % capacity_;
  size_--;

  return item;
}

template<typename T>
std::vector<T> CircularBuffer<T>::get_snapshot() const {
  std::unique_lock<std::mutex> lock(mutex_);
  std::vector<T> result;
  result.reserve(size_);

  size_t current = head_;
  for (size_t i = 0; i < size_; ++i) {
    result.push_back(buffer_[current]);
    current = (current + 1) % capacity_;
  }

  return result;
}

template<typename T>
bool CircularBuffer<T>::empty() const {
  std::unique_lock<std::mutex> lock(mutex_);
  return size_ == 0;
}

template<typename T>
size_t CircularBuffer<T>::size() const {
  std::unique_lock<std::mutex> lock(mutex_);
  return size_;
}

template<typename T>
void CircularBuffer<T>::clear() {
  std::unique_lock<std::mutex> lock(mutex_);
  head_ = 0;
  tail_ = 0;
  size_ = 0;
}

// Explicit instantiation for CompletedTaskInfo
template class CircularBuffer<CompletedTaskInfo>;
