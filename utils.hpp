#ifndef _UTILS_H_
#define _UTILS_H_ 1

/*
 * utils.h
 * 
 * Copyright 2012 Nicholas Leonard <nick@nikopia.org>
 *  
 */
 
#include <iostream>
#include <string>
#include <termios.h>
#include <unistd.h>

void SetStdinEcho(bool enable = true);
std::string getUserInput(const std::string& message, const std::string& default_value);
std::string trim(std::string const& source, char const* delims = " \t\r\n");

#endif
