test_threading: GeneralHashFunctions/GeneralHashFunctions.o test_threading.o threading.o database.o utils.o -lpthread -lpq -lpqtypes -lboost_serialization
	g++ -O2 -std=gnu++0x GeneralHashFunctions/GeneralHashFunctions.o  database.o utils.o threading.o test_threading.o \
	-o test_threading -lpthread -lpq -lpqtypes -lboost_serialization -lPocoNet -lPocoFoundation
	
test_threading.o: threading.hpp database.hpp test_threading.cpp 
	g++ -std=gnu++0x -c test_threading.cpp

threading.o: database.hpp threading.cpp threading.hpp GeneralHashFunctions/GeneralHashFunctions.h 
	g++ -std=gnu++0x -c threading.cpp 
	
database.o: database.cpp database.hpp utils.hpp 
	g++ -std=gnu++0x -c database.cpp

utils.o: utils.cpp utils.hpp
	g++ -std=gnu++0x -c utils.cpp
	
