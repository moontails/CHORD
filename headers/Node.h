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
  int nodeID, predecessor=0;
  std::vector<int> data;
  std::array<std::pair<int,int>,8> finger_table;

  static int message_counter;
  static std::vector<int> member_list;
  static std::vector<bool> check_flag;
  static std::vector<bool> redo_flag;
  static std::vector<std::queue<std::string>> messageQ;

  int find_successor(const int);
  int find_predecessor(const int);
  int find_closest_preceding_finger(int);

  void init();
  int init_ft(const int);

  void join(const int);
  void update_others();
  void update_ft(const int, const int);
};

#endif  /* NODE_H_ */
