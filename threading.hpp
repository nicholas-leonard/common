#ifndef _THREADING_H_
#define _THREADING_H_ 1

/*
 * threading.hpp
 * 
 * Copyright 2012 Nicholas Leonard <nick@nikopia.org>
 * 
 * Ref. : http://www.hongliangjie.com/2011/09/16/random-number-generation-with-c-0x-in-gcc/
 * http://docs.python.org/library/queue.html
 * http://www.simetric.co.uk/si_time.htm
 * http://en.wikipedia.org/wiki/Actor_model
 * 
 * Compile with : g++ -std=gnu++0x 
 */

#include <pthread.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <map>
#include <unordered_map>
#include <queue>
#include <vector>
#include <utility>
#include <random>
#include <ctime>
#include <sys/time.h>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <errno.h>
#include "GeneralHashFunctions/GeneralHashFunctions.h"
#include "database.hpp"

// http://www.boost.org/doc/libs/1_51_0/libs/serialization/doc/index.html :
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <cstring>

#define HEADER_LENGTH 8

typedef size_t hash_t;

class ID {
	std::string _type;
	int _key;
public :
	ID() {};
	ID(std::string type, int key) :
		_type(type),
		_key(key) {
	};
	ID(const ID& id) :
		_type(id._type),
		_key(id._key) {
	};
	const std::string& getType() const {
		return _type;
	};
	const int& getKey() const {
		return _key;
	};
	bool operator ==(const ID& id) {
		return ((id._type == _type) && (id._key == _key));
	};
	std::string str() const {
		std::stringstream ss;
		ss << _type << " " << _key;
		return ss.str();
	};
	hash_t getHash() const {
		return DJBHash (str());
	};
	void operator=(const ID& id) {
		_type = id._type;
		_key = id._key;
	};
private :
	friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
        ar & _type;
        ar & _key;
    }
};


class Thread {
	/* A class that represents a pthread. The code in run() is executed
	 * in a phtread when start() is called by the parent thread. The 
	 * pthread is joined when join() is called by a the parent thread. 
	 * Join() returns when the thread has existed. */
public:
	Thread() {};
	/** Returns true if the thread was successfully started, false if there was an error starting the thread */
	void start() {
		if (pthread_create(&_thread, NULL, internalThreadInit, this) != 0) 
			throw StartError("Could not start thread");
	};
	/** Will not return until the internal thread has exited. */
	void join() {
	  (void) pthread_join(_thread, NULL);
	};
	class StartError : public std::runtime_error {
	public:
		StartError(const std::string& message) : 
			std::runtime_error(message) { };
	};
	virtual ~Thread() {
		std::cout << "Destroying thread" << std::endl;
	};
protected:
	/** Implement this method in your subclass with the code you want your thread to run. */
	virtual void run() = 0;
private:
   static void* internalThreadInit(void * This) {
	   ((Thread *)This)->run(); 
	   return NULL;
	}
    pthread_t _thread;
};

class Message {
	/* The analog of a packet. It has a header for indicating the source
	 * and destination of the message. The content of the message is 
	 * stored in _data. Messages can be sent between processes when 
	 * _data can be cast into a string and _archive_size is greater than
	 * 0. Messages are then serialized for transmission and deserialized
	 * on the receiving end. */
protected :
	std::string _message_type;
	int _message_priority;
	ID _source_actor_id;
	ID _destination_actor_id;
	void* _data;
	size_t _archive_size;
public :
	Message() {};
	Message(std::string message_type, const ID& source_actor_id, const ID& destination_actor_id, void* data, int message_priority=100, size_t archive_size=0) : 
		_message_type(message_type),
		_message_priority(message_priority),
		_source_actor_id(source_actor_id),
		_destination_actor_id(destination_actor_id),
		_data(data),
		_archive_size(archive_size) {
	};
	const std::string& getType() { 
		return _message_type; 
	};
	ID& getSource() { 
		return _source_actor_id;
	};
	ID& getDestination() { 
		return _destination_actor_id;
	};
	void setSource(const ID& source_actor_id) {
		_source_actor_id = source_actor_id;
	}; 
	void setDestination(const ID& destination_actor_id) {
		_destination_actor_id = destination_actor_id;
	}; 
	void* getData() {
		return _data;
	};
	std::string* getDataString() {
		return (std::string*)_data;
	};
	void setDataString(std::string* data_str) {
		_data = (void*) data_str;
		_archive_size = data_str->size();
	};
	void setData(void* data) {
		_data = data;
	};
	bool isArchive() {
		return (_archive_size!=0);
	};
	size_t getArchiveSize() {
		return _archive_size;
	};
	class MessageException : public std::runtime_error {
	public:
		MessageException(const std::string& message) : 
			std::runtime_error(message) { };
	};
private :
	friend class boost::serialization::access;
    // When the class Archive corresponds to an output archive, the
    // & operator is defined similar to <<.  Likewise, when the class Archive
    // is a type of input archive the & operator is defined similar to >>.
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
        ar & _message_type;
        ar & _message_priority;
        ar & _source_actor_id;
        ar & _destination_actor_id;
        ar & _archive_size;
    }
};

static inline 
void get_abstimeout(timespec& abstimeout, unsigned long timeout_msec) {
	timeval now;
	gettimeofday(&now, NULL);
	abstimeout.tv_sec = now.tv_sec + timeout_msec/1000;
	abstimeout.tv_nsec = (now.tv_usec * 1000) 
		+ ((timeout_msec % 1000) * 1000000);
	if (abstimeout.tv_nsec >= 1000000000) {
		abstimeout.tv_sec++;
		abstimeout.tv_nsec -= 1000000000;
	}
}

class Queue {
	/* A Queue that can be used to communicate Messages between threads. 
	 * It uses a similar interface to the python Queues. Queues have a 
	 * max length. A muted is used to allow only one message to be 
	 * put() or get() at any one time, i.e. for thread safe concurrency. 
	 * put blocks waiting for the not_full_cond condition when the Queue
	 * is full. Conversely, it get() blocks waiting for the 
	 * not_empty_cond when the Queue is empty. shutdown() blocks waiting
	 * for the empty_cond. */
protected :
	size_t max_length;
	pthread_mutex_t mutex;
	pthread_cond_t not_empty_cond;
	pthread_cond_t not_full_cond;
	pthread_cond_t empty_cond;
	std::priority_queue<Message*> queue; 
	bool queue_closed;
	bool shutdown_now;
	bool finish;
public :
	//Queue();
	Queue(size_t _max_length=200, bool _finish=true) :
		max_length(_max_length),
		queue_closed(false),
		shutdown_now(false),
		finish(_finish) {
		// init conditions :
		if (pthread_cond_init(&not_empty_cond, NULL) != 0) 
			throw std::runtime_error("pthread condition 'not_empty' initialization failed");
		if (pthread_cond_init(&not_full_cond, NULL) != 0) {
			pthread_cond_destroy(&not_empty_cond);
			throw std::runtime_error("pthread condition 'not_full' initialization failed");
		}
		if (pthread_cond_init(&empty_cond, NULL) != 0) {
			pthread_cond_destroy(&not_empty_cond);
			pthread_cond_destroy(&not_full_cond);
			throw std::runtime_error("pthread condition 'empty' initialization failed");
		}
		// init mutex :
		if (pthread_mutex_init(&mutex, NULL) != 0) {
			pthread_cond_destroy(&not_empty_cond);
			pthread_cond_destroy(&not_full_cond);
			pthread_cond_destroy(&empty_cond);
			throw std::runtime_error("pthread mutex initialization failed");
		}
	};
	Message* get(bool block=true, unsigned long timeout=0) {
		// convert timeout milliseconds to timespec structure:
		//std::cout << " get1 " << queue.size() << std::endl;
		timespec abstimeout;
		if (block && (timeout > 0))
			get_abstimeout(abstimeout, timeout);
		
		pthread_mutex_lock(&mutex);
		
		if ((!block) && queue.empty()) {
			pthread_mutex_unlock(&mutex);
			throw EmptyException("Queue is empty during non-blocking get()");
		}
		
		int ret = 0;
		while (queue.empty() && (ret != ETIMEDOUT)) {
			if (timeout > 0) {
				ret = pthread_cond_timedwait(&not_empty_cond, &mutex, &abstimeout);
			} else {
				pthread_cond_wait(&not_empty_cond, &mutex);
			}
		}
		if (ret == ETIMEDOUT) {
			pthread_mutex_unlock(&mutex);
			throw EmptyException("Queue is still empty after 'timeout' msec");
		}
		
		Message* msg = queue.top();
		queue.pop();
		
		if (queue.size() == (max_length-1))
			pthread_cond_broadcast(&not_full_cond);
		if (queue.empty())
			pthread_cond_signal(&empty_cond);
		
		pthread_mutex_unlock(&mutex);
		//std::cout << " get2 " << queue.size() << std::endl;
		return msg;
	};
	void put(Message* msg, bool block=true, unsigned long timeout=0) {
		// convert timeout milliseconds to timespec structure:
		//std::cout << " put1 " << queue.size() << std::endl;
		timespec abstimeout;
		if (block && (timeout > 0))
			get_abstimeout(abstimeout, timeout);
		
		pthread_mutex_lock(&mutex);
		
		if ((!block) && (queue.size() == max_length)) {
			pthread_mutex_unlock(&mutex);
			throw FullException("Queue is full during non-blocking put()");
		}
		
		int ret = 0;
		while ((queue.size() == max_length) && (ret != ETIMEDOUT)) {
			if (timeout > 0) {
				ret = pthread_cond_timedwait(&not_full_cond, &mutex, &abstimeout);
			} else {
				pthread_cond_wait(&not_full_cond, &mutex);
			}
		}
		if (ret == ETIMEDOUT) {
			pthread_mutex_unlock(&mutex);
			throw FullException("Queue is still full after 'timeout' msec");
		}
		
		queue.push(msg);
		
		if (!queue.empty())
			pthread_cond_broadcast(&not_empty_cond);
		
		pthread_mutex_unlock(&mutex);
		//std::cout << " put2 " << queue.size() << std::endl;
	};
	void shutdown() {
		pthread_mutex_lock(&mutex);
		if (queue_closed || shutdown_now) 
			throw ShutdownException("Queue is already being shutdown.");
		queue_closed = true;
		if (finish) {
			while(!queue.empty()) {
				pthread_cond_wait(&empty_cond, &mutex);
			}
		}
		shutdown_now = true;
		pthread_mutex_unlock(&mutex);
		if (pthread_cond_broadcast(&not_empty_cond) != 0)
			throw ConditionException("Queue.not_empty_cond pthread_cond_broadcast Error");
		if (pthread_cond_broadcast(&not_full_cond) != 0)
			throw ConditionException("Queue.not_full_cond pthread_cond_broadcast Error");	
	};
	void put_timeout(Message* msg, unsigned long timeout) {
		put(msg, true, timeout);
	};
	void put_nowait(Message* msg) {
		put(msg, false, 0);
	};
	Message* get_timeout(unsigned long timeout){
		return get(true, timeout);
	};
	Message* get_nowait() {
		return get(false, 0);
	};
	~Queue() {};
	class ConditionException : public std::runtime_error {
	public:
		ConditionException(const std::string& message) : 
			std::runtime_error(message) { };
	};
	class FullException : public std::runtime_error {
	public:
		FullException(const std::string& message) : 
			std::runtime_error(message) { };
	};
	class EmptyException : public std::runtime_error {
	public:
		EmptyException(const std::string& message) : 
			std::runtime_error(message) { };
	};
	class ShutdownException : public std::runtime_error {
	public:
		ShutdownException(const std::string& message) : 
			std::runtime_error(message) { };
	};
};

struct hashed : public std::unary_function<hash_t, size_t>
{
	size_t operator()(const hash_t& h) const
	{
		return h;
	}	
};

class ProcessProxy {
	/* Represents a process made up of many Actors. */
public :
	virtual void send(Message* message, bool block, int timeout);
	class ProcessProxyException : public std::runtime_error {
	public:
		ProcessProxyException(const std::string& message) : 
			std::runtime_error(message) { };
	};
};

class InterProcessProxy : public ProcessProxy { 
	/* Used to communicate messages between processes. This is a proxy
	 * for an external process with ID _process_id. Its _writer_queue
	 * is used to send messages to a SocketWriter Actor which is 
	 * responsible for sending the message over a socket to a single 
	 * other socket managed by a SocketReader Actor in the external 
	 * process with ID _process_id. */
protected :
	Queue* _writer_queue;
	ID	_process_id;
public :	
	InterProcessProxy(Queue* writer_queue, const ID& process_id) :
		_writer_queue(writer_queue),
		_process_id(process_id) {
	};
	void send(Message* message, bool block, int timeout) {
		_writer_queue->put(message, block, timeout);
	};
};

class Connector;

class Actor {
	/* An Actor in the sense of the Actor Model. It can establish new 
	 * connections with other Actors using the Connector. The Connector
	 * instantiates ProcessProxies for communication with other Actors.
	 * These other Actors can be in the current process or in external 
	 * ones. Each Actor has a _get_queue which it uses to get() Messages
	 * that are addressed to it. */
protected :
	// address of this actor :
	ID _id;
	// index on (process_hash)
	std::unordered_map<hash_t, ProcessProxy*, hashed> _process_map;
	typedef std::unordered_map<hash_t, ProcessProxy*, hashed>::iterator process_iterator;
	// index on (actor_hash)
	std::unordered_map<hash_t, ID*, hashed> _actor_map;
	typedef std::unordered_map<hash_t, ID*, hashed>::iterator actor_iterator;
	// index on (actor_type_hash, )
	std::unordered_map<hash_t, std::vector<int>*, hashed> _actor_type_map;
	typedef std::unordered_map<hash_t, std::vector<int>*, hashed>::iterator actor_type_iterator;
	Connector* _connector;
	Queue* _get_queue;
public : 
	typedef std::vector<int>::const_iterator actor_key_iterator;
	Actor(Connector* connector, ID id, Queue* get_queue) :
		_id(id),
		_connector(connector),
		_get_queue(get_queue) {
	};
	hash_t getHash() {
		return _id.getHash();
	};
	const ID& getID() const {
		return _id;
	};
	const ID& getProcessID(ID& actor_id);
protected :
	void put(Message* message, bool block=true, int timeout=0);
	void put_timeout(Message* msg, unsigned long timeout) {
		put(msg, true, timeout);
	};
	void put_nowait(Message* msg) {
		put(msg, false, 0);
	};
	virtual void serialize(Message* message) {};
	const std::vector<int>& getActorVector(const std::string& actor_type);
public :
	class ActorException : public std::runtime_error {
	public:
		ActorException(const std::string& message) : 
			std::runtime_error(message) { };
	};
};

class ThreadActor : public Actor, public Thread {
	/* A ThreadActor is an Actor and a Thread. */
public :
	ThreadActor(Connector* connector, ID id, Queue* message_queue) :
		Actor(connector, id, message_queue) {
		std::cout << _id.str() << " initialized" << std::endl;
	};
};

class SocketWriter : public ThreadActor {
	/* A SocketWriter is responsible for forwarding Messages received 
	 * through its queue to a single external process through a socket.
	 * A SocketWriter is instantiated by the Connector singleton when 
	 * Actors of the process request a ProcessProxy for sending messages
	 * to Actors of an external process. Each process has a maximum of 
	 * one instance of SocketWriter for each external process it needs 
	 * to communicate with. This minimizes the amount of sockets.
	 * For each process-to->process pair, there is a ProcessProxy and 
	 * a SocketWriter on the client side, and a SocketReader on the 
	 * server side. These 3 instances are dedicated to this directed 
	 * communication link. 
	 * ProcessProxies and SocketWriters share a reference to the same 
	 * Queue. Actors use the ProcessProxy to put() Messages in the Queue
	 * which the SocketWriter get()s. This Queue manages the concurrency
	 * between Actors for the socket. And thus abtrasts away the socket
	 * into what is essentially a Queue. */
protected :
	struct sockaddr_in _server_address;
	int _socket_fd;
public :
	SocketWriter(struct sockaddr_in server_address, Connector* connector, ID id, Queue* message_queue) :
		ThreadActor(connector, id, message_queue),
		_server_address(server_address),
		_socket_fd(socket(AF_INET, SOCK_STREAM, 0)) { 
		if (_socket_fd < 0) 
			throw SocketWriterException("ERROR opening socket");
	};
	void connect() {
		if (::connect(_socket_fd,(struct sockaddr *) &_server_address, sizeof(_server_address)) < 0) 
			throw SocketWriterException("ERROR connecting");
	};
	void write(std::string& buffer_str) {
		if (::write(_socket_fd, buffer_str.c_str(), buffer_str.size()) < 0) {
			switch(errno) {
				case EWOULDBLOCK: throw SocketWriterException("EWOULDBLOCK The file descriptor fd refers to a socket and has been marked nonblocking (O_NONBLOCK), and the write would block. POSIX.1-2001 allows either error to be returned for this case, and does not require these constants to have the same value, so a portable application should check for both possibilities.");
					break;
				case EBADF: throw SocketWriterException("EBADF fd is not a valid file descriptor or is not open for writing.");
					break;
				case EDESTADDRREQ: throw SocketWriterException("EDESTADDRREQ fd refers to a datagram socket for which a peer address has not been set using connect(2).");
					break;
				case EFAULT: throw SocketWriterException("EFAULT buf is outside your accessible address space.");
					break;
				case EFBIG: throw SocketWriterException("EFBIG An attempt was made to write a file that exceeds the implementation-defined maximum file size or the process's file size limit, or to write at a position past the maximum allowed offset.");
					break;
				case EINTR: throw SocketWriterException("EINTR The call was interrupted by a signal before any data was written; see signal(7).");
					break;
				case EINVAL: throw SocketWriterException("EINVAL fd is attached to an object which is unsuitable for writing; or the file was opened with the O_DIRECT flag, and either the address specified in buf, the value specified in count, or the current file offset is not suitably aligned.");
					break;
				case EIO: throw SocketWriterException("EIO A low-level I/O error occurred while modifying the inode.");
					break;
				case ENOSPC: throw SocketWriterException("ENOSPC The device containing the file referred to by fd has no room for the data.");
					break;
				case EPIPE: throw SocketWriterException("EPIPE fd is connected to a pipe or socket whose reading end is closed. When this happens the writing process will also receive a SIGPIPE signal. (Thus, the write return value is seen only if the program catches, blocks or ignores this signal.)");
					break;
				default:
					std::stringstream ss;
					ss << "Unknown WRITE error #" << errno << ": " << strerror(errno);
					throw SocketWriterException(ss.str());
					break;
			}
		}
	};
	void run() {
		connect();
		while (true) {
			Message* message = _get_queue->get();
			// Serialize the message first so we know how large it is.
			std::ostringstream archive_stream;
			boost::archive::text_oarchive archive(archive_stream);
			archive << (*message);
			std::string message_str = archive_stream.str();
			// Format the header.
			std::ostringstream header_stream;
			header_stream << std::setw(HEADER_LENGTH) << std::hex << message_str.size();
			if (!header_stream || header_stream.str().size() != HEADER_LENGTH)
				throw SocketWriterException("Error with HEADER_LENGTH");
			std::string header_str = header_stream.str();
			write(header_str);
			write(message_str);
			if (message->isArchive()) {
				// Assume the message._data was already serialized :
				std::string* data_str = message->getDataString();
				write(*data_str);
				delete data_str;
			};
			delete message;
		};
		close(_socket_fd);	
	};
	class SocketWriterException : public std::runtime_error {
	public:
		SocketWriterException(const std::string& message) : 
			std::runtime_error(message) { };
	};
};

class ProcessActor;

class Connector {
	/* The Connector is a singleton in the scope of a process, i.e. 
	 * there is one Connector instance per Process. It is used by Actors
	 * to establish connections with any Actors listed in the database. 
	 * It maintains a list of ProcessProxies for all active 
	 * communication links with remote and the local processes. When an 
	 * Actor of the current process first requests a ProcessProxy, it 
	 * is either returned from the list of active ProcessProxies, or it
	 * is first instantiated with its associated SocketWriter and remote
	 * SocketReader. Actors also hold a list of references to active 
	 * ProcessProxies such that the Connector need only be called once 
	 * per Actor to process communication request. This minimizes 
	 * concurrency overhead since most calls to Connector are protected 
	 * by mutexes. The Connector also holds a DatabaseHandler, i.e. a 
	 * database connection object, for communication with the central
	 * ProcessActor and ThreadActor repository. */
protected :
	pthread_mutex_t mutex;
	// index on (process_hash, )
	std::unordered_map<hash_t, ProcessProxy*, hashed> _process_map;
	typedef std::unordered_map<hash_t, ProcessProxy*, hashed>::iterator process_iterator;
	// index on (actor_hash, )
	std::unordered_map<hash_t, ID*, hashed> _actor_map;
	typedef std::unordered_map<hash_t, ID*, hashed>::iterator actor_iterator;
	// index on (actor_type_hash, )
	std::unordered_map<hash_t, std::vector<int>*, hashed> _actor_type_map;
	typedef std::unordered_map<hash_t, std::vector<int>*, hashed>::iterator actor_type_iterator;
	// Tables :
	std::string _schema_name;
	DatabaseHandle _db;
	int socket_writer_key_gen;
	ID _process_actor_id;
public :	
	Connector(std::string conn_string, std::string schema_name) :
		_schema_name(schema_name),
		_db(conn_string),
		socket_writer_key_gen(0) {
		// init mutex :
		if (pthread_mutex_init(&mutex, NULL) != 0)
			throw std::runtime_error("pthread mutex initialization failed");
	};
	void setThisProcess(ProcessActor* process_actor);
	const ID& getMyProcess() const {
		return _process_actor_id;
	};
	struct sockaddr_in getSocketAddress(std::string process_type, int process_key) {
		pthread_mutex_lock(&mutex);
		struct sockaddr_in socket_address = get_SocketAddress(process_type, process_key);
		pthread_mutex_unlock(&mutex);
		return socket_address;
	};
	ProcessProxy* getProcessProxy(const ID& process_id) {
		pthread_mutex_lock(&mutex);
		ProcessProxy* pp = get_ProcessProxy(process_id);
		pthread_mutex_unlock(&mutex);
		return pp;
	};
	ID* getProcessID(const ID& actor_id) {
		pthread_mutex_lock(&mutex);
		actor_iterator pi = _actor_map.find(actor_id.getHash());
		if (pi == _actor_map.end()) {
			ID* process_id = select_ProcessID(actor_id.getType(), actor_id.getKey());
			_actor_map.insert(std::make_pair(actor_id.getHash(), process_id));
			pthread_mutex_unlock(&mutex);
			return process_id;
		}
		ID* process_id = pi->second;
		pthread_mutex_unlock(&mutex);
		return process_id;
	};
	std::vector<int>* getActorVector(const std::string& actor_type) {
		pthread_mutex_lock(&mutex);
		actor_type_iterator pi = _actor_type_map.find(DJBHash(actor_type));
		if (pi == _actor_type_map.end()) {
			std::vector<int>* actor_vector = select_ActorVector(actor_type);
			_actor_type_map.insert(std::make_pair(DJBHash(actor_type), actor_vector));
			pthread_mutex_unlock(&mutex);
			return actor_vector;
		}
		std::vector<int>* actor_vector = pi->second;
		pthread_mutex_unlock(&mutex);
		return actor_vector;
	};
	class ConnectorException : public std::runtime_error {
	public:
		ConnectorException(const std::string& message) : 
			std::runtime_error(message) { };
	};
private :
	ProcessProxy* get_ProcessProxy(const ID& process_id) {
		process_iterator pi = _process_map.find(process_id.getHash());
		if (pi == _process_map.end()) {
			// get the socket address of the unknown process :
			struct sockaddr_in socket_address = get_SocketAddress(process_id.getType(), process_id.getKey());
			// init a SocketWriter :
			Queue* writer_queue = new Queue();
			ID actor_id("Socket Writer", socket_writer_key_gen++);
			SocketWriter* socket_writer = new SocketWriter(socket_address, this, actor_id, writer_queue);
			socket_writer->start();
			// Inform the ProcessActor of the new Thread :
			ID connector_id("Connector", 0);
			Message* msg = new Message("New Thread", connector_id, _process_actor_id, (void*)socket_writer);
			ProcessProxy* process_actor = get_ProcessProxy(_process_actor_id);
			process_actor->send(msg, true, 0);
			// Create a InterProcessProxy associated with the socket writer :
			InterProcessProxy* ipp = new InterProcessProxy(writer_queue, process_id);
			_process_map.insert(std::make_pair(process_id.getHash(), ipp));
			return ipp;
		}
		ProcessProxy* pp = pi->second;
		return pp;
	};
	std::vector<int>* select_ActorVector(const std::string& actor_type) {
		std::string command = ""
		"SELECT actor_key "
		"FROM \"" + _schema_name + "\".actor_process "
		"WHERE actor_type = %varchar;";
		// std::cout << command << " " << actor_type << std::endl; 
		PGresult* res = _db.execute(command.c_str(), actor_type.c_str());
		size_t num_tuple = PQntuples(res);
		if (num_tuple == 0)
			throw ConnectorException("Actor Type is unavailable");
		PGint4 actor_key;
		std::vector<int>* actor_vector = new std::vector<int>();
		for (size_t i = 0; i != num_tuple; i++) {
			PQgetf(res, i, "%int4", 0, &actor_key);
			actor_vector->push_back(actor_key);
		}
		PQclear(res);
		return actor_vector;
	};
	ID* select_ProcessID (std::string actor_type, int actor_key) {
		std::string command = ""
		"SELECT process_type, process_key "
		"FROM \"" + _schema_name + "\".actor_process "
		"WHERE (actor_type, actor_key) = (%varchar, %int4);";
		// std::cout << command << " " << actor_type << " " << actor_key << std::endl; 
		PGresult* res = _db.execute(command.c_str(), actor_type.c_str(), actor_key);
		//std::cout << command << actor_type.c_str() << actor_key << " " << PQntuples(res) << std::endl;
		if (PQntuples(res) != 1)
			throw ConnectorException("Actor is unavailable");
		PGvarchar process_type;
        PGint4 process_key;
		PQgetf(res, 0, "%varchar, %int4", 0, &process_type, 1, &process_key);
		std::string process_type_str(process_type);
		PQclear(res);
		return new ID(process_type_str, process_key);
	};
	struct sockaddr_in get_SocketAddress(std::string process_type, int process_key) {
		std::string command = ""
		"SELECT listen_address, listen_port "
		"FROM \"" + _schema_name + "\".process "
		"WHERE (process_type, process_key) = (%varchar, %int4)";
		// std::cout << command << " " << process_type << " " << process_key << std::endl; 
		PGresult* res = _db.execute(command.c_str(), process_type.c_str(), process_key);
		if (PQntuples(res) != 1)
			throw ConnectorException("Process is unavailable");
		PGinet listen_address;
        unsigned short listen_port;
		PQgetf(res, 0, "%inet, %int2", 0, &listen_address, 1, &listen_port);
		char ip[80];
		struct sockaddr* server_addr = (struct sockaddr *)listen_address.sa_buf;
		
		// converting a PGinet to an IPv4 or IPv6 address string
		getnameinfo(server_addr, listen_address.sa_buf_len, ip, sizeof(ip), NULL, 0, NI_NUMERICHOST);

		// The inet data type does not store a port.
		((struct sockaddr_in *)server_addr)->sin_port = htons(listen_port);

		//connect(socket_fd, server_addr, listen_address.sa_len);
		struct sockaddr_in server_address = *(sockaddr_in*)server_addr;
		PQclear(res);
		return server_address;
	};
};

class SocketReader : public ThreadActor {
	/* A SocketReader is spawned by the SocketAcceptor upon receipt of 
	 * a socket connection request. There is only one instance of 
	 * SocketReader for each process<-to-remoteprocess socket. This 
	 * means that for each remote process that needs to communicate with
	 * Actors of this process, only one SocketReader is allowed. 
	 * It forwards messages received through its socket to the 
	 * destination Actors. The only way to communicate with it is 
	 * through its socket, even though it has a _get_queue (that it 
	 * doesn't use). */
	// TODO: read message types to see if concerns self
	//       handle broken sockets
protected :
	int _socket_fd;
	struct sockaddr_in _client_address;
	char _header_buffer[HEADER_LENGTH];
	std::vector<char> _message_buffer;
	std::vector<char> _data_buffer;
public :
	SocketReader(int socket_fd, struct sockaddr_in client_address, Connector* connector, ID id) :
		ThreadActor(connector, id, NULL),
		_socket_fd(socket_fd),
		_client_address(client_address) {
	};
	void run () {
		int n;
		while (true) {
			n = read(_socket_fd, _header_buffer, HEADER_LENGTH);
			if (n < 0) 
				throw SocketReaderException("ERROR reading from socket");
			// Determine the length of the serialized message:
			std::istringstream is(std::string(_header_buffer, HEADER_LENGTH));
			std::size_t message_size = 0;
			if (!(is >> std::hex >> message_size))
				throw SocketReaderException("INVALID header length");
			// Read message of length message_size from socket :
			_message_buffer.resize(message_size);
			n = read(_socket_fd, &_message_buffer[0], _message_buffer.size());
			if (n < 0) 
				throw SocketReaderException("ERROR reading from socket");
			// Unserialize message:
			std::string archive_data(&_message_buffer[0], _message_buffer.size());
			std::istringstream archive_stream(archive_data);
			Message* message = new Message();
			try {
				boost::archive::text_iarchive archive(archive_stream);
				archive >> (*message);
			} catch (boost::archive::archive_exception& e) {
				std::stringstream ss;
				ss << "ERROR reading message archive : " << e.what();
				throw SocketReaderException(ss.str());
			}
			if (message->isArchive()) {
				// Read messagedata from socket:
				_data_buffer.resize(message->getArchiveSize());
				n = read(_socket_fd, &_data_buffer[0], _data_buffer.size());
				std::string* data_str = new std::string(&_data_buffer[0], _data_buffer.size());
				if (n < 0) 
					throw SocketReaderException("ERROR reading from socket");
				message->setData((void *)data_str);
			}
			put(message);
		};
		close(_socket_fd);	
	};
	class SocketReaderException : public std::runtime_error {
	public:
		SocketReaderException(const std::string& message) : 
			std::runtime_error(message) { };
	};
};

class SocketAcceptor : public ThreadActor {
	/* The SocketAcceptor accepts socket connection requests from remote 
	 * processes. It spawns a SocketReader instance for each new socket 
	 * connection request. There is only one SocketAcceptor instance per 
	 * process, which makes it a singleton. When a new SocketReader 
	 * thread is spawned, it informs the ProcessActor. 
	 * TODO : 
	 * what if the client address in already assigned a SocketReader?
	 * how do we communicate with the socket acceptor? */
protected :
	int _socket_fd;
	struct sockaddr_in _server_address;
	unsigned short _port;
	std::unordered_map<hash_t, SocketReader*, hashed> socket_reader_map;
	int _key_generator;
public :
	SocketAcceptor(short port, Connector* connector, ID id, Queue* message_queue) :
		ThreadActor(connector, id, message_queue),
		_port(htons(port)),
		_key_generator(0) {
		_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
		if (_socket_fd < 0) 
			throw SocketAcceptorException("ERROR opening socket");
		memset(&_server_address, '\0', sizeof(_server_address));
		_server_address.sin_family = AF_INET;
		_server_address.sin_addr.s_addr = INADDR_ANY;
		_server_address.sin_port = _port;
		if (bind(_socket_fd, (struct sockaddr *) &_server_address, sizeof(_server_address)) < 0) 
			throw SocketAcceptorException("ERROR on binding");  
	};
	SocketAcceptor(struct sockaddr_in socket_address, Connector* connector, ID id, Queue* message_queue) :
		ThreadActor(connector, id, message_queue),
		_server_address(socket_address),
		_port(socket_address.sin_port),
		_key_generator(0) {
		_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
		if (_socket_fd < 0) 
			throw SocketAcceptorException("ERROR opening socket");
		if (bind(_socket_fd, (struct sockaddr *) &_server_address, sizeof(_server_address)) < 0) 
			throw SocketAcceptorException("ERROR on binding");  
	};
	void run() {
		// Accept new connection requests
		int new_socket_fd;
		socklen_t clilen;
		struct sockaddr_in client_address;
		listen(_socket_fd, 5);
		clilen = sizeof(client_address);
		while (true) {
			new_socket_fd = accept(_socket_fd, (struct sockaddr *) &client_address, &clilen);
			if (new_socket_fd < 0) 
				throw SocketAcceptorException("ERROR on accept");
			// Launch a SocketReader for each new connection request :
			ID id("Socket Reader", _key_generator++);
			SocketReader* socket_reader = new SocketReader(new_socket_fd, client_address, _connector, id);
			socket_reader->start();
			// Inform the ProcessActor of the new Thread :
			Message* msg = new Message("New Thread", _id, _connector->getMyProcess(), (void*)socket_reader);
			put(msg);
		}
	};
	~SocketAcceptor() {
		close(_socket_fd);
	};
	class SocketAcceptorException : public std::runtime_error {
	public:
		SocketAcceptorException(const std::string& message) : 
			std::runtime_error(message) { };
	};
};

class ProcessActor : public Actor, public ProcessProxy {
	/* A ProcessActor represents the main thread of the process and 
	 * the process as a collection of ThreadActors. It is itself an 
	 * Actor. It is the main reason the Actor class was separated from
	 * the Thread class. It shares a common interface with the 
	 * InterProcessProxy, i.e. the ProcessProxy interface.
	 * */
protected :
	// ThreadActors :
	std::unordered_map<hash_t, ThreadActor*, hashed> _thread_map;
	typedef std::unordered_map<hash_t, ThreadActor*, hashed>::iterator thread_iterator;
	// Queues :
	std::unordered_map<hash_t, Queue*, hashed> _queue_map;
	typedef std::unordered_map<hash_t, Queue*, hashed>::iterator queue_iterator;
public :
	ProcessActor(Connector* connector, ID id, Queue* message_queue) :
		Actor(connector, id, message_queue) {
		// Adds its Queue to _queue_map so others can access it :
		_queue_map.insert(std::make_pair(_id.getHash(), message_queue));
		// Init the SocketAcceptor:
		ID sa_id("Socket Acceptor", 0);
		Queue* sa_queue = new Queue();
		_queue_map.insert(std::make_pair(sa_id.getHash(), sa_queue));
		struct sockaddr_in my_sockaddr = _connector->getSocketAddress(_id.getType(), _id.getKey());
		SocketAcceptor* sa = new SocketAcceptor(my_sockaddr, _connector, sa_id, sa_queue);
		_thread_map.insert(std::make_pair(sa->getHash(), sa));
		// Add self to Connector:
		_connector->setThisProcess(this);
	};
	void send(Message* message, bool block, int timeout) {
		ID& destination = message->getDestination();
		// Note: Assumes message is destined to this process :
		queue_iterator qi = _queue_map.find(destination.getHash());
		if (qi == _queue_map.end()) {
			std::stringstream ss;
			ss << "Unknown destination Address : " << destination.str();
			throw ProcessProxyException(ss.str());
		}
		Queue* actor_queue = qi->second;
		actor_queue->put(message);
	};
	void handle_new_thread(Message* message) {
		ThreadActor* thread = (ThreadActor*)message->getData();
		_thread_map.insert(std::make_pair(thread->getHash(), thread));
		delete message;
	};
	void handle_thread_exit(Message* message) {
		ThreadActor* thread = (ThreadActor*)message->getData();
		_thread_map.insert(std::make_pair(thread->getHash(), thread));
		delete message;
	};
	void start() {
		for (thread_iterator it = _thread_map.begin(); it != _thread_map.end(); it++)
			it->second->start();
		std::cout << "threads started" << std::endl;
	};
	void run() {
		while (true) {
			Message* message = _get_queue->get();
			const std::string& message_type = message->getType();
			if (message_type == "New Thread") {
				handle_new_thread(message);
			} else if (message_type == "Thread Exit") {
				handle_thread_exit(message);
			}
		}
	};
};


/* class MasterProcess {
protected :
	
public :
	void updateCluster();
};*/

#endif
