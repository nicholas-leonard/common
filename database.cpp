/*
 * database.cpp
 * 
 * Copyright 2012 Nicholas Leonard <nick@nikopia.org>
 * 
 */


#include "database.hpp"


using std::string; using std::cout; using std::cin; using std::endl;

DatabaseHandle::DatabaseHandle() {
	DatabaseHandle(askConnectionString("postgres", "localhost", "postgres"));
}

DatabaseHandle::DatabaseHandle(string connection_string) {
	conn_string = connection_string;
	conn = PQconnectdb(conn_string.c_str());
	if (PQstatus(conn) != CONNECTION_OK) {
		string error_message = PQerrorMessage(conn);
		PQfinish(conn);
		throw DatabaseException("Connection to database failed: \n" + error_message);
	}
	PQinitTypes(conn);
}

string DatabaseHandle::getConnectionString() {
	return conn_string;
}

std::string DatabaseHandle::askConnectionString(string dbname, string host, string user) {
	cout << "Time to prepare the database connection string :" << endl;
	string connection_string = "dbname=" + getUserInput("Enter dbname", dbname);
	connection_string.append(" host=" + getUserInput("Enter host", host));
	connection_string.append(" user=" + getUserInput("Enter user", user));
	SetStdinEcho(false);
	cout << "Enter password: ";
	string password;
	getline(cin,password);
	connection_string.append(" password=" + password);
	cout << endl;
	SetStdinEcho(true); 
	return connection_string;
}

PGresult* DatabaseHandle::execute(const char *cmdspec, ...) {
	va_list ap;
	PGresult *res;
	if (conn == NULL) {
		cout << "DB Connection lost, re-establishing connection" << endl;
		DatabaseHandle(conn_string);
	}
	va_start(ap, cmdspec);
	res = PQexecvf(conn, cmdspec, ap);
	va_end(ap);
	
	if(!res)
		throw DatabaseException(PQgeterror());

	return res;
}

