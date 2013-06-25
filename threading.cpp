/*
 * threading.cpp
 * 
 * Copyright 2012 Nicholas Leonard <nick@nikopia.org>
 * 
 */
 

#include "threading.hpp"

void ProcessProxy::send(Message* message, bool block, int timeout) {};

bool Actor::outboundMessage(ID& process_id) {
	return (process_id.getHash() != _connector->getMyProcess().getHash());
};

ProcessProxy& Actor::getProcessProxy(ID& process_id){
	// Get process_proxy :
	hash_t process_hash = process_id.getHash();
	process_iterator pi = _process_map.find(process_hash);
	ProcessProxy* process_proxy;
	if (pi == _process_map.end()) {
		process_proxy = _connector->getProcessProxy(process_id);
		_process_map.insert(std::make_pair(process_hash, process_proxy));
	} else
		process_proxy = pi->second;
	return *process_proxy;
};

ID& Actor::getProcessID(ID& actor_id) {
	actor_iterator pi = _actor_map.find(actor_id.getHash());
	if (pi == _actor_map.end()) {
		ID* process_id = new ID(*_connector->getProcessID(actor_id));
		_actor_map.insert(std::make_pair(actor_id.getHash(), process_id));
		return *process_id;
	}
	ID& process_id = (*pi->second);
	return process_id;
};

const std::vector<ID>& Actor::getLocalActorIDs(const std::string actor_type) {
	std::vector<ID>* actor_vector;
	actor_type_iterator ati = _local_actor_type_map.find(DJBHash(actor_type.c_str()));
	if (ati == _local_actor_type_map.end()) {
		actor_vector = _connector->getLocalActorIDs(actor_type);
		_local_actor_type_map.insert(std::make_pair(DJBHash(actor_type), actor_vector));
	} 
	else
		actor_vector = ati->second;
	return *actor_vector;
};

const std::vector<ID>& Actor::getGlobalActorIDs(const std::string actor_type) {
	std::vector<ID>* actor_vector;
	actor_type_iterator ati = _global_actor_type_map.find(DJBHash(actor_type.c_str()));
	if (ati == _global_actor_type_map.end()) {
		actor_vector = _connector->getGlobalActorIDs(actor_type);
		_global_actor_type_map.insert(std::make_pair(DJBHash(actor_type), actor_vector));
	}
	else
		actor_vector = ati->second;
	return *actor_vector;
};

void Connector::setThisProcess(ProcessActor* process_actor) {
	_process_actor_id = process_actor->getID();
	_process_map.insert(std::make_pair(_process_actor_id.getHash(), process_actor));
};
