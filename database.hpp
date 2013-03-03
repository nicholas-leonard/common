#ifndef _DATABASE_H_
#define _DATABASE_H_ 1

/*
 * database.h
 * 
 * Copyright 2012 Nicholas Leonard <nick@nikopia.org>
 * 
 */
 
#include "utils.hpp"
#include <postgresql/libpq-fe.h>
#include <stdexcept>
#include <vector>
#include <fstream>


extern "C" {
extern int PQinitTypes(PGconn *conn);
extern int PQclearTypes(PGconn *conn);
}

#include <libpqtypes.h>



class DatabaseHandle {
private :
	std::string conn_string;
public : //temp
	PGconn *conn;
public :
	DatabaseHandle();
	DatabaseHandle(std::string connection_string);
	std::string getConnectionString();
	static std::string askConnectionString(std::string dbname, std::string host, std::string user);
	PGresult* execute(const char *cmdspec, ...);
	class DatabaseException : public std::runtime_error {
	public:
		DatabaseException(const std::string& message) : 
			std::runtime_error(message) { };
	};
	void close();
	void connect();
};


#endif
