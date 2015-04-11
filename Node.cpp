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
std::mutex mtx1;
// to map command to a numeral
std::map<std::string, int> commandMap;
std::vector<std::thread> thread_pool(32);
//defining the static variables
int Node::message_counter = 0;
bool Node::redo_flag = false;
std::vector<int> Node::member_list;
std::vector<bool> Node::check_flag(32, false);
std::vector<std::queue<std::string>> Node::messageQ;

void node_runner(const int node_id);

void display(const std::vector<int>& v)
{
  for (int i=0; i<v.size();i++)
  {
    cout << v[i] << endl;
  }
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
        Node::redo_flag = true;
        Node::check_flag[node_id] = true;
        std::cout << "And its flag is set" << std::endl;
        // append to member list to keep track of alive nodes in the system
        Node::member_list.push_back(node_id);
        std::sort(Node::member_list.begin(), Node::member_list.end());
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
    if(Node::redo_flag)
    {

      Node::redo_flag = false;

      Node::
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
