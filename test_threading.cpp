/*
 * test_threadning.cpp
 * 
 * Copyright 2012 Nicholas Leonard <nick@nikopia.org>
 * 
 */
#include "threading.hpp"

// A serializable object that will be sent over the wire
class PrintMessage {
private :
	std::string _message;
	friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version) {
        ar & _message;
    }
public :
	PrintMessage(){}
	PrintMessage(std::string message) :
		_message(message) {
	}
	void print(std::string actor_address) {
		std::cout << "Print message #" << _message << " from thread #" << actor_address << std::endl;
	}
};


class PrintActor;
class ForwardActor;
class DistributeActor;

// A actor responsible for printing messages
class PrintActor : public ThreadActor {
public :
	PrintActor(Connector* connector, ID id, Queue* message_queue) :
		ThreadActor(connector, id, message_queue)  {
	};
	~PrintActor() {
		try {
			_get_queue->shutdown();
		} catch (Queue::ShutdownException E) {
			std::cout << "Warning: " << E.what() << " Ignored." << std::endl;
		}
	};
	void run() {
		while (true) {
			try {
				Message* msg = _get_queue->get();
				if ((msg->getType() == "Print") and msg->isArchive()) {
					PrintMessage* pm = new PrintMessage();
					std::string* data_str = msg->getDataString();
					std::istringstream archive_stream(*data_str);
					boost::archive::text_iarchive archive(archive_stream);
					archive >> (*pm);
					pm->print(_id.str());
					delete msg;
					delete pm;
					delete data_str;
				} else if (msg->getType() == "Join Now") {
					std::cout << "Print exiting" << std::endl;
					pthread_exit(NULL);
				} else {
					std::cout << "Message " << msg->getType() << " is not unknown" << std::endl;
				}
			} catch(Queue::EmptyException E) {
				std::cout << E.what() << std::endl;
			}
		}
	}
};

class ForwardActor : public ThreadActor {
public :
	ForwardActor(Connector* connector, ID id, Queue* message_queue) :
		ThreadActor(connector, id, message_queue)  {
	};
	void serialize(Message* msg) {
		if (msg->getType() == "Print") {
			PrintMessage* pm = (PrintMessage*)msg->getData();
			std::ostringstream archive_stream;
			boost::archive::text_oarchive archive(archive_stream);
			archive << (*pm);
			std::string* pm_str = new std::string(archive_stream.str());
			delete pm;
			msg->setDataString(pm_str);
		};
	};
	void run() {
		const std::vector<int>& actor_vector = getActorVector("Print");
		std::vector<int>::const_iterator it = actor_vector.begin();
		while (true) {
			Message* msg = _get_queue->get(); 
			if (msg->getType() == "Print") {
				if (it == actor_vector.end())
					it = actor_vector.begin();
				int actor_key = *it;
				it++;
				ID dest_actor_id("Print", actor_key);
				msg->setDestination(dest_actor_id);
				put(msg);
			} else if (msg->getType() == "Join Now") {
				delete msg;
				if (_id.getKey() == 0) {
					std::cout << "Forward tells prints to join now" << std::endl;
					for(it = actor_vector.begin(); it != actor_vector.end(); it++) {
						ID dest_actor_id("Print", *it);
						// No data to serialize ...:
						Message* msg = new Message("Join Now", _id, dest_actor_id, NULL, 1000);
						put(msg);
					};
				};	
				std::cout << "Forward exiting" << std::endl;
				pthread_exit(NULL);
			} else {
				std::cout << "Message " << msg->getType() << " is not unknown" << std::endl;
			}
		}
	}
	~ForwardActor() {
		try {
			_get_queue->shutdown();
		} catch (Queue::ShutdownException E) {
			std::cout << "Warning: " << E.what() << " Ignored." << std::endl;
		}
	};
};

class DistributeActor : public ThreadActor {
public :
	DistributeActor(Connector* connector, ID id, Queue* message_queue) :
		ThreadActor(connector, id, message_queue) {
	};
	void run() {
		std::cout << _id.str() << " running" << std::endl;
		const std::vector<int>& actor_vector = getActorVector("Forward");
		size_t num_forward_actor = actor_vector.size();
		for (int i=0; i!=10000; i++) {
			for(std::vector<int>::const_iterator it = actor_vector.begin(); it!=actor_vector.end(); it++) {
				int actor_key = *it;
				std::stringstream ss;
				ss << _id.getKey() << "Hello World" << i;
				PrintMessage* pm = new PrintMessage(ss.str());
				ID dest_actor_id("Forward", actor_key);
				Message* msg = new Message("Print", _id, dest_actor_id, (void*)pm, 10+(actor_key % num_forward_actor));
				put(msg);
			}
		}
		size_t done_received(1);
		if (_id.getKey() == 0) {
			size_t num_print_actor = getActorVector("Distribute").size();
			// Barrier :
			std::cout << "Distribute Barrier commencing" << std::endl;
			while (done_received != num_print_actor) {
				std::cout << "distribute" << done_received <<"/"<<num_print_actor<< std::endl;
				Message* msg = _get_queue->get();
				std::cout << "distribute got it" << std::endl;
				if (msg->getType() == "Done")
					done_received += 1;
				delete msg;
			}
			std::cout << "Distribute Barrier propagating" << std::endl;
			for(std::vector<int>::const_iterator it = actor_vector.begin(); it!=actor_vector.end(); it++) {
				ID dest_actor_id("Forward", *it);
				Message* msg = new Message("Join Now", _id, dest_actor_id, NULL, 1000);
				put(msg);
			};
		} else {
			// inform master peer that work is complete (Barrier):
			ID dest_actor_id("Distribute", 0);
			Message* msg = new Message("Done", _id, dest_actor_id, NULL, 1001);
			put(msg);
		}
		std::cout << "Distribute exiting" << std::endl;
		pthread_exit(NULL);
	}
	~DistributeActor() {
		try {
			_get_queue->shutdown();
		} catch (Queue::ShutdownException E) {
			std::cout << "Warning: " << E.what() << " Ignored." << std::endl;
		}
	};
};

class ServerProcess: public ProcessActor {
public :
	ServerProcess(std::string conn_string, ID id) :
		ProcessActor(new Connector(conn_string, "test"), id, new Queue()) {
		const std::vector<int>& print_vector = getActorVector("Print");
		for (std::vector<int>::const_iterator it = print_vector.begin(); it!=print_vector.end(); it++) {
			ID pa_id("Print", *it);
			Queue* q = new Queue();
			PrintActor* pa = new PrintActor(_connector, pa_id, q);
			// Adds its Queue to _queue_map so others can access it :
			_queue_map.insert(std::make_pair(pa_id.getHash(), q));
			_thread_map.insert(std::make_pair(pa_id.getHash(), pa));
		}	
		std::cout << "Server Process initialized" << std::endl;	
	}
};

class ClientProcess: public ProcessActor {
public :
	ClientProcess(std::string conn_string, ID id) :
		ProcessActor(new Connector(conn_string, "test"), id, new Queue()) {
		const std::vector<int>& distribute_vector = getActorVector("Distribute");
		for (std::vector<int>::const_iterator it = distribute_vector.begin(); it!=distribute_vector.end(); it++) {
			ID da_id("Distribute", *it);
			Queue* q = new Queue();
			DistributeActor* da = new DistributeActor(_connector, da_id, q);
			// Adds its Queue to _queue_map so others can access it :
			_queue_map.insert(std::make_pair(da_id.getHash(), q));
			_thread_map.insert(std::make_pair(da_id.getHash(), da));	
		}
		const std::vector<int>& forward_vector = getActorVector("Forward");
		for (std::vector<int>::const_iterator it = forward_vector.begin(); it!=forward_vector.end(); it++) {
			ID fa_id("Forward", *it);
			Queue* q = new Queue();
			ForwardActor* fa = new ForwardActor(_connector, fa_id, q);
			// Adds its Queue to _queue_map so others can access it :
			_queue_map.insert(std::make_pair(fa_id.getHash(), q));
			_thread_map.insert(std::make_pair(fa_id.getHash(), fa));
		}		
		std::cout << "Client Process initialized" << std::endl;	
	}
};

int main(int argc, char **argv) {
	if (argc != 2)
		std::cout << "Usage: test_threading [server|client]" << std::endl;
	std::string mode(argv[1]);
	std::string conn_string(DatabaseHandle::askConnectionString("udem", "localhost", "nicholas"));
	if (mode == "server") {
		ID server_id("Server", 0);
		ServerProcess sp(conn_string, server_id);
		sp.start();
		sp.run();
	} else if (mode == "client") {
		ID client_id("Client", 0);
		ClientProcess cp(conn_string, client_id);
		cp.start();
		cp.run();
	};
};
