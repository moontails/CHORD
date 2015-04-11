/*
 * MessageHandler.h
 *
 *  Created on: Mar 8, 2015
 *      Author: moontails, emch2
 */

#ifndef MESSAGEHANDLER_H_
#define MESSAGEHANDLER_H_

#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <chrono>

#define DELIM ':'

using namespace std;

class MessageHandler
{
public:
	static std::string serialize(std::string inputMessage);
	static std::vector<std::string> deserialize(std::string inputMessage);
	static std::string deserializeB(std::string inputMessage);
	static std::string serialize_map(std::map<int, std::pair<int,std::chrono::system_clock::time_point>>);
	static std::map<int, std::pair<int,std::chrono::system_clock::time_point>> deserialize_map(std::string);
};

#endif /* MESSAGEHANDLER_H_ */
