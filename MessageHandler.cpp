/*
* Storage.h
*
*  Created on: Mar 8, 2015
*      Author: moontails, emch2
*/


#include "headers/MessageHandler.h"
#include <iostream>
#include <algorithm>
#include <string>
#include <map>
#include <vector>
#include <sstream>
#include <chrono>

using namespace std;
using std::chrono::system_clock;

/*
 * Function to serialize the input message into a string for transport across the communication channel
 */
std::string MessageHandler::serialize(std::string inputMessage)
{
	std::string message = inputMessage;

	std::replace(message.begin(), message.end(), ' ', DELIM);

	return message;
}

/*
 * Function to deserialize upon receiving a message from the communication channel
 */
std::vector<std::string> MessageHandler::deserialize(std::string inputMessage)
{
	std::string token;
	std::vector<std::string> command;

	size_t pos = 0;
	while((pos = inputMessage.find(DELIM)) != std::string::npos){
		token = inputMessage.substr(0,pos);
		command.push_back(token);
		inputMessage.erase(0,pos+1);
	}

	command.push_back(inputMessage);
	return command;
}

/*
 * Function to serialize a map into a string for transport across the communication channel
 */
std::string MessageHandler::serialize_map(std::map<int, std::pair<int,std::chrono::system_clock::time_point>> map)
{

	std::ostringstream oss;
	std::map<int, std::pair<int,std::chrono::system_clock::time_point>>::iterator it;
	std::time_t t;

	for(it = map.begin(); it != map.end(); ++it){
		t = system_clock::to_time_t(it->second.second);
		oss<<it->first<<"#"<<it->second.first<<"#"<<t<<"_";

	}

	return oss.str();
}

/*
 * Function to deserialize a string received from the communication channel into a map
 */
std::map<int, std::pair<int,std::chrono::system_clock::time_point>> MessageHandler::deserialize_map(std::string s)
{
	std::map<int, std::pair<int,std::chrono::system_clock::time_point>> map;
	std::string token;
	size_t pos = 0;
	std::string token2;
	std::chrono::system_clock::time_point tp;
	std::time_t t;
	std::vector<std::string> entry;

	while((pos = s.find("_")) != std::string::npos){
		token = s.substr(0,pos);
		size_t pos_token = 0;
		while((pos_token = token.find("#")) != std::string::npos){
		    token2 = token.substr(0,pos_token);
			entry.push_back(token2);
			token.erase(0,pos_token+1);
		}

		entry.push_back(token);

		t = stoi(entry[2]);
		tp = system_clock::from_time_t(t);
		map[stoi(entry[0])] = std::make_pair(stoi(entry[1]), tp);
		entry.erase(entry.begin(),entry.end());
		s.erase(0, pos+1);
	}
	return map;
}
