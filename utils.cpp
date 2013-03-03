/*
 * utils.cpp
 * 
 * Copyright 2012 Nicholas Leonard <nick@nikopia.org>
 * 
 */
 
#include "utils.hpp"

using std::string; using std::cout; using std::cin; using std::endl;

// http://stackoverflow.com/questions/1413445/read-a-password-from-stdcin :
void SetStdinEcho(bool enable)
{
#ifdef WIN32
    HANDLE hStdin = GetStdHandle(STD_INPUT_HANDLE); 
    DWORD mode;
    GetConsoleMode(hStdin, &mode);

    if( !enable )
        mode &= ~ENABLE_ECHO_INPUT;
    else
        mode |= ENABLE_ECHO_INPUT;

    SetConsoleMode(hStdin, mode );

#else
    struct termios tty;
    tcgetattr(STDIN_FILENO, &tty);
    if( !enable )
        tty.c_lflag &= ~ECHO;
    else
        tty.c_lflag |= ECHO;

    (void) tcsetattr(STDIN_FILENO, TCSANOW, &tty);
#endif
}

//http://www.cplusplus.com/forum/general/28663/
string getUserInput(const string& message, const string& default_value) {
	string value;
	cout << message << " (" << default_value << ") : ";
	getline(cin,value);
	if ( value == "" )
		return default_value;
	return value;
}

//http://www.adp-gmbh.ch/cpp/config_file.html
std::string trim(std::string const& source, char const* delims) {
	std::string result(source);
	std::string::size_type index = result.find_last_not_of(delims);
	if(index != std::string::npos)
	result.erase(++index);

	index = result.find_first_not_of(delims);
	if(index != std::string::npos)
	result.erase(0, index);
	else
	result.erase();
	return result;
}

/*int main(int argc, char **argv) {
	string username = getUserInput("Enter username", "Nicholas");
	SetStdinEcho(false);
	cout << "Enter password: ";
	string password;
	getline(cin,password);
	cout << endl;
	SetStdinEcho(true); 
	string message = getUserInput("Enter message", "Hello Worlds");
	cout << "Username : " << username << endl;
	cout << "Password : " << password << endl;
	cout << "Message : " << message << endl;
	
}*/
