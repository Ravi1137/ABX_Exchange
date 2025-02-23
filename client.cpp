#include <iostream>
#include <fstream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <sstream>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <set>
#include <nlohmann/json.hpp>
#include <chrono>
using namespace std;

using json = nlohmann::json;

struct Packet {
    std::string symbol;
    char buy_sell_indicator;
    uint32_t quantity;
    uint32_t price;
    uint32_t sequence;
};

std::vector<Packet> parsed_packets;
std::mutex json_mutex;

const int PORT = 3000;
const char* SERVER_IP = "127.0.0.1"; 
const int PACKET_SIZE = 17; 

// Function for converting big-endian to host byte order
uint32_t big_endian_to_host(uint32_t net_value) {
    return ntohl(net_value);
}

// function to get the current timestamp include milliseconds and nanoseconds
std::string get_current_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    auto now_tm = std::localtime(&now_time_t);
    
    auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()) % 1000;

    std::ostringstream oss;
    oss << std::put_time(now_tm, "%Y-%m-%d %H:%M:%S")
        << "." << std::setw(3) << std::setfill('0') << milliseconds.count()
        << "." << std::setw(3) << std::setfill('0') << nanoseconds.count(); 
    return oss.str();
}

//Function for converting byte to hexadecimal value
std::string byte_to_hex(unsigned char byte) {
    std::ostringstream oss;
    oss << std::hex << std::setw(2) << std::setfill('0') << (int)byte;
    return oss.str();
}

// Function for Writing the log
void log_message(std::ofstream& log_file, const std::string& message, bool to_console = true) {
    std::string timestamp = get_current_timestamp();
    std::string log_entry = "[" + timestamp + "] " + message;
    if (to_console) {
        std::cout << log_entry << std::endl;
    }
    log_file << log_entry << std::endl;
}

// Function for connecting to a socket
int create_and_connect_socket(const char* ip, int port, std::ofstream& log_file) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        log_message(log_file, "Could not create socket: " + std::string(strerror(errno)));
        std::cin.get();
        return -1; 
    }

    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    server.sin_addr.s_addr = inet_addr(ip);

    struct timeval timeout;      
    timeout.tv_sec = 2;  // Set timeout to 2 seconds
    timeout.tv_usec = 0; 
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout)) < 0) {
        log_message(log_file, "Failed to set socket options: " + std::string(strerror(errno)));
        close(sock);
        std::cin.get();
        return -1; 
    }

    log_message(log_file, "Attempting to connect to server at " + std::string(ip) + ":" + std::to_string(port) + "...");

    if (connect(sock, (struct sockaddr*)&server, sizeof(server)) < 0) {
        log_message(log_file, "Connection failed: " + std::string(strerror(errno)));
        close(sock);
        std::cin.get();
        return -1; 
    }

    log_message(log_file, "Connected to server successfully.");
    return sock;
}

// function to Send data request to server
void send_request(int sock, std::ofstream& log_file, uint8_t call_type, uint32_t missing_sequence = 0) {
    uint8_t payload[2] = { call_type, static_cast<uint8_t>(missing_sequence) };
    log_message(log_file, "Preparing to send payload to server: CallType=" + std::to_string(call_type));

    // Send the payload to the server
    if (send(sock, payload, sizeof(payload), 0) < 0) {
        log_message(log_file, "Send failed: " + std::string(strerror(errno)));
        close(sock);
        std::cin.get();
        exit(1);
    }
    log_message(log_file, "Payload sent successfully: CallType=" + std::to_string(call_type));
}

// Global mutex and condition variable for thread safety
std::mutex sequence_mutex;
std::mutex parsing_mutex; // mutex for parsing
std::condition_variable cv;
std::queue<std::string> packet_queue; // Queue for processed packets
std::set<uint32_t> processed_sequences; // To track processed packet sequences
std::vector<uint32_t> sequence_numbers; // Vector to track sequence numbers
bool stop_parsing = false;

// Function to save packets as JSON
void save_to_json(const std::vector<Packet>& packets) {
    std::cout << "Writing in file Output.json" << std::endl;
    std::lock_guard<std::mutex> lock(json_mutex); // Lock to prevent concurrent modification
    json json_output = json::array();
    for (const auto& packet : packets) {
        json_output.push_back({
            {"Symbol", packet.symbol},
            {"BuySellIndicator", std::string(1, packet.buy_sell_indicator)},
            {"Quantity", packet.quantity},
            {"Price", packet.price},
            {"Sequence", packet.sequence}
        });
    }
    std::ofstream json_file("output.json", std::ios::trunc);
    if (json_file.is_open()) {
        json_file << json_output.dump(4);
        json_file.close();
    } else {
        std::cerr << "Failed to create or open output.json" << std::endl;
        std::cin.get();
    }
}

// function for parsing received packets
void parse_packets(std::ofstream& log_file) {
    while (true) {
        std::string packet;
        
        // Waiting for packets to be available in the queue
        {
            std::unique_lock<std::mutex> lock(parsing_mutex);
            cv.wait(lock, [] { return !packet_queue.empty(); });
            packet = packet_queue.front();
            packet_queue.pop();
        }

        // Parsing the packet
        char symbol[5] = {0}; 
        memcpy(symbol, packet.c_str(), 4);
        char buy_sell_indicator = packet[4];
        uint32_t quantity, price, packet_sequence;
        memcpy(&quantity, packet.c_str() + 5, sizeof(quantity));
        memcpy(&price, packet.c_str() + 9, sizeof(price));
        memcpy(&packet_sequence, packet.c_str() + 13, sizeof(packet_sequence));

        quantity = big_endian_to_host(quantity); 
        price = big_endian_to_host(price);
        packet_sequence = big_endian_to_host(packet_sequence);

        {
            std::lock_guard<std::mutex> lock(sequence_mutex);
            if (processed_sequences.find(packet_sequence) != processed_sequences.end()) {
                log_message(log_file, "Duplicate packet ignored: Sequence=" + std::to_string(packet_sequence));
                continue; // Ignore this packet as it's already processed
            }
            processed_sequences.insert(packet_sequence); // Mark this packet as processed
            sequence_numbers.push_back(packet_sequence); // Store the sequence number
        }

        {
            std::lock_guard<std::mutex> lock(json_mutex);
            parsed_packets.push_back({symbol, buy_sell_indicator, quantity, price, packet_sequence});
        }

        log_message(log_file, "Received Packet After Processing : Symbol=" + std::string(symbol)
                      + ", Buy/Sell=" + buy_sell_indicator
                      + ", Quantity=" + std::to_string(quantity)
                      + ", Price=" + std::to_string(price)
                      + ", Sequence=" + std::to_string(packet_sequence)
                    );
        if (packet_queue.empty()) {
            break;
        }
    }
}

// Function for receiveing and enqueueing packets for processing
void receive_packets(int sock, std::ofstream& log_file) {
    char buffer[1024];
    ssize_t bytes_received;

    while (true) {
        bytes_received = recv(sock, buffer, sizeof(buffer), 0);
        if (bytes_received > 0) {
            std::ostringstream raw_data_stream;
            raw_data_stream << "Raw received data (hex):    ";
            for (size_t i = 0; i < bytes_received; ++i) {
                raw_data_stream << byte_to_hex(buffer[i]) << " ";
                // Insert a newline after every 17 bytes for easily log view
                if ((i + 1) % PACKET_SIZE == 0) {
                    raw_data_stream << "\n"; 
                }
            }
            log_message(log_file, raw_data_stream.str());

            // Enqueuing each packet for processing
            for (size_t i = 0; i < bytes_received; i += PACKET_SIZE) {
                if (i + PACKET_SIZE <= bytes_received) {
                    std::lock_guard<std::mutex> lock(parsing_mutex); // Lock for thread safety
                    packet_queue.push(std::string(buffer + i, PACKET_SIZE)); // Push packet into queue
                }
            }
            //cv.notify_all(); // Notify the parser thread
        } 
        else {
            log_message(log_file, bytes_received <= 0 ? "Server disconnected." : "Receive failed: " + std::string(strerror(errno)));
            cv.notify_all(); // Notify the parser thread
            // std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            // stop_parsing = true;
            // cv.notify_all();
            break;
        }
    }
}

// function to Check for missing sequences
std::vector<uint32_t> check_missing_sequences(const std::vector<uint32_t>& sequence_numbers, std::ofstream& log_file) {
    std::vector<uint32_t> missing_sequences;
    uint32_t expected_sequence = 1;

    for (uint32_t seq : sequence_numbers) {
        while (expected_sequence < seq) {
            log_message(log_file, "Detected missing sequence: " + std::to_string(expected_sequence));
            missing_sequences.push_back(expected_sequence++);
        }
        expected_sequence = seq + 1;
    }

    return missing_sequences;
}

// function taht requests missing sequences from the server
void request_missing_sequences(int sock, const std::vector<uint32_t>& missing_sequences, std::ofstream& log_file) {
    for (uint32_t missing_sequence : missing_sequences) {
        send_request(sock, log_file, 2, missing_sequence);
    }
}

// function for Closin g the socket communication
void close_socket(int sock, std::ofstream& log_file) {
    if (close(sock) < 0) {
        log_message(log_file, "Failed to close socket: " + std::string(strerror(errno)));
    } else {
        log_message(log_file, "Socket closed successfully.");
    }
}

// Main function
int main() {
    std::ofstream log_file("log.txt", std::ios::trunc);
    if (!log_file.is_open()) {
        std::cerr << "Failed to open log file." << std::endl;
        std::cin.get();
        return 1;
    }

    int sock = create_and_connect_socket(SERVER_IP, PORT, log_file);
    if (sock == -1) {
        log_file.close();
        return 1; // Exit if socket creation/connection failed
    }

    // Sending initial request that is 1 to server
    send_request(sock, log_file, 1);

    // Creating a thread for parsing packets
    std::thread parser_thread(parse_packets, std::ref(log_file));

    // Start receiving packets
    receive_packets(sock, log_file);

    // Check for missing sequences after receiving packets
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::vector<uint32_t> missing_sequences = check_missing_sequences(sequence_numbers, log_file);

    if (!missing_sequences.empty()) {
        log_message(log_file, "Missing packets detected, attempting to retrieve...");
        int new_sock = create_and_connect_socket(SERVER_IP, PORT, log_file); // Reconnect

        if (new_sock != -1) {
            std::thread parser_thread(parse_packets, std::ref(log_file));
            request_missing_sequences(new_sock, missing_sequences, log_file);
            receive_packets(new_sock, log_file);
            if (parser_thread.joinable()) {
                parser_thread.join();
            }
            close_socket(new_sock, log_file);
        } else {
            log_message(log_file, "Reconnection failed, aborting missing packet request.");
        }
    }

    // Ensure all packets are parsed before proceeding
    if (parser_thread.joinable()) {
        parser_thread.join();
    }
    // Sorting packets by sequence number to save to JSON
    //log_message(log_file, "Sorting packets and saving to JSON...");
    std::sort(parsed_packets.begin(), parsed_packets.end(), [](const Packet& a, const Packet& b) {
        return a.sequence < b.sequence;
    });

    save_to_json(parsed_packets);
    // Close socket
    close_socket(sock, log_file);
    log_file.close();

    return 0;
}

