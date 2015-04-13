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
  int nodeID, predecessor;
  std::vector<int> data;
  std::array<int,8> finger_table;

  static int message_counter;
  static std::vector<int> member_list;
  static std::vector<bool> check_flag;
  static std::vector<bool> redo_flag;
  static std::vector<std::queue<std::string>> messageQ;
};

#endif  /* NODE_H_ */
