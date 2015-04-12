/*
* Node.cpp
*
*  Created on: Mar 8, 2015
*      Author: moontails, emch2
*/

#include "headers/Node.h"
#include "headers/MessageHandler.h"
#include <algorithm>
#include <thread>
#include <mutex>
#include <map>

using namespace std;

// mutex lock to gurantee mutual exclusion while reading the message queue
std:vector<std::mutex> mtx_locks(32);
// to map command to a numeral
std::map<std::string, int> commandMap;
std::vector<std::thread> thread_pool(32);
//defining the static variables
int Node::message_counter = 0;
std::vector<int> Node::member_list;
std::vector<bool> Node::check_flag(32, false);
std::vector<bool> Node::redo_flag(32, false);
std::vector<std::queue<std::string>> Node::messageQ(32);

void node_runner(const int node_id);

void display(const std::vector<int>& v)
{
  for (int i=0; i<v.size();i++)
  {
    cout << v[i] << endl;
  }
}

void update_member_list(const int node_id)
{
  //Add to member list
  Node::member_list.push_back(node_id);
  //Sort this list
  std::sort(Node::member_list.begin(), Node::member_list.end());

  //Set flag so that all nodes in the system recompute its finger table
  for (int i=0; i < Node::member_list.size(); i++)
  {
    Node::redo_flag[Node::member_list[i]] = true;
  }
}

void compute_finger_table(const int node_id, std::array<std::pair<int,int>,8>& finger_table)
{
  int finger;
  bool found;
  for(int i=0; i<8; i++)
  {
    found = false;
    finger = (node_id + (2^i) ) % 256;

    for(int i=0; i < Node::member_list.size(); i++)
    {
      if( Node::member_list[i] > finger)
      {
        finger_table[i] = Node::member_list[i];
        found = true;
        break;
      }
    }
    if(!found)
    {
      finger_table[i] = Node::member_list[0];
    }
  }
}

int find_predecessor(const int node_id)
{
  for(int i=0; i < Node::member_list.size(); i++)
  {
    if(node_id == Node::member_list[i])
      break;
    }
    return i == 0 ? Node::member_list[Node::member_list.size()-1] : i-1 ;
  }

  void listener()
{
  // to store user input command
  int node_id, key;
  std::string inputCommand, inputMessage;
  std::vector<std::string> inputMessageVector;

  while(1)
  {
    std::cout << "\nEnter the command: " << std::endl;
    std::getline(std::cin, inputCommand);

    // serialize and deserialize the message
    inputMessage = MessageHandler::serialize(inputCommand);
    inputMessageVector = MessageHandler::deserialize(inputMessage);

    // obtain the command invoked
    std::string command = inputMessageVector.front();

    switch(commandMap[command])
  {
    case 1: //join
    node_id = std::stoi(inputMessageVector[1]);
    std::cout << "\nNode: " << node_id << " is joining the system" << std::endl;
    Node::check_flag[node_id] = true;
    std::cout << "And its flag is set" << std::endl;
    update_member_list(node_id);
    thread_pool[node_id] = std::thread(node_runner, node_id);
    display(Node::member_list);
    break;

    case 2: //find
    node_id = std::stoi(inputMessageVector[1]);
    key = std::stoi(inputMessageVector[2]);
    std::cout << "\nFound"<< std::endl;
    break;

    case 3: //leave
    node_id = std::stoi(inputMessageVector[1]);
    std::cout << "\nNode: " << node_id << " is leaving the system" << std::endl;
    Node::check_flag[node_id] = false;
    break;

    case 4: //show
    node_id = std::stoi(inputMessageVector[1]);
    std::cout << "\nShowing keys at Node: " << node_id << std::endl;
    break;

    case 5: //show-all
    std::cout << "\nShowing all keys"<< std::endl;
    break;

    default:
    std::cout << "\nPlease check your input" << std::endl;
    break;
  }
}
}


void node_runner(const int node_id)
{
  Node newnode;
  newnode.nodeID = node_id;

  while(Node::check_flag[node_id])
  {
    // when a new node joins, all other nodes should recompute their finger tables
    if(Node::redo_flag[node_id])
    {
      compute_finger_table(newnode.nodeID, newnode.finger_table);
      Node::redo_flag[node_id] = false;

    }
  }
}
/*
* Function - Main function to -
- initialize the node info from config file.
- map commands/operation to a numeral.
- display the node information.
- spawn three threads to simulate distributed key value store.
*/
int main(int argc, char *argv[])
{
  //Initially the system consists of node 0
  int node_id = 0;
  Node::redo_flag = true;
  Node::check_flag[node_id] = true;
  Node::member_list.push_back(node_id);
  //map the supported commands
  commandMap["join"] = 1;
  commandMap["find"] = 2;
  commandMap["leave"] = 3;
  commandMap["show"] = 4;
  commandMap["show-all"] = 5;

  std::cout << "Hello World" << std::endl;

  // spawn the three threads
  std::thread coordinator_thread (listener);
  thread_pool[node_id] = std::thread(node_runner, node_id);

  // wait for them to complete their execution
  coordinator_thread.join();
  for(int i=0; i<32; i++)
  {
    thread_pool[i].join();
  }


  return 0;
}
