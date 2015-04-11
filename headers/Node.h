/*
* Node.h
*
*  Created on: April 4, 2015
*      Author: moontails
*/


#ifndef NODE_H_
#define NODE_H_

#include <iostream>
#include <string.h>
#include <array>
#include <vector>
#include <queue>
#include <map>

using namespace std;

class Node
{

public:
  static int message_counter;
  static bool redo_flag;
  static std::vector<int> member_list;
  static std::vector<bool> check_flag;
  static std::vector<std::queue<std::string>> messageQ;
private:
  int nodeID;
  std::map<int, int> data;
  std::array<std::pair<int,int>,8> finger_table;
};

#endif  /* NODE_H_ */
