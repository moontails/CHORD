/*
* Node.cpp
*
*  Created on: Mar 8, 2015
*      Author: moontails, emch2
*/

#include "headers/Node.h"
#include "headers/MessageHandler.h"
#include <algorithm>
#include <sstream>
#include <thread>
#include <mutex>
#include <map>
#include <cmath>

// mutex lock to gurantee mutual exclusion while reading the message queue
std::vector<std::mutex> mtx_locks(256);
// to map command to a numeral
std::map<std::string, int> commandMap, nodeCommandMap;
std::vector<std::thread> thread_pool(256);
//defining the static variables
int Node::message_counter = 0;
std::vector<int> Node::member_list;
std::vector<bool> Node::check_flag(256, false);
std::vector<bool> Node::redo_flag(256, false);
std::vector<std::queue<std::string>> Node::messageQ(256);
std::vector<std::pair<int,std::string>> listenerQ;

void node_runner(const int node_id);

void pop_message(const int node_id)
{
  mtx_locks[node_id].lock();
  Node::messageQ[node_id].pop();
  mtx_locks[node_id].unlock();
}

void push_message(const int node_id, const std::string inputMessage)
{
  mtx_locks[node_id].lock();
  Node::messageQ[node_id].push(inputMessage);
  mtx_locks[node_id].unlock();
}

int find_helper(const int key, const std::array<int,8> finger_table)
{
  std::vector<int> temp;

  for(int i =0; i<8; i++)
  {
    if(finger_table[i] <= key)
    {
      temp.push_back(finger_table[i]);
    }
  }

  if(temp.size() == 0)
  {
    return finger_table[0];
  }

  std::sort(temp.begin(), temp.end());

  return temp[temp.size()-1];
}

void display(const std::vector<int>& v)
{
  std::cout << "Membership List: ";
  for (int i=0; i<v.size();i++)
  {
    cout << v[i] << " ";
  }
  std::cout << std::endl;
}

void update_member_list(const int node_id, const bool flag=false)
{

  if(!flag)
  {
    //Add to member list
    Node::member_list.push_back(node_id);
    //Sort this list
    std::sort(Node::member_list.begin(), Node::member_list.end());
  }
  else
  {
    //find position
    auto it = std::find(Node::member_list.begin(), Node::member_list.end(), node_id);

    //remove from the list
    if(it != Node::member_list.end())
    {
      Node::member_list.erase(it);
    }
  }

  //Set flag so that all nodes in the system recompute its finger table
  for (int i=0; i < Node::member_list.size(); i++)
  {
    Node::redo_flag[Node::member_list[i]] = true;
  }

  //std::cout << "in update" << std::endl;
}

void compute_finger_table(const int node_id, std::array<int,8>& finger_table)
{
  int finger;
  bool found;
  for(int i=0; i<8; i++)
  {
    found = false;
    finger = (node_id + static_cast<int>(pow(2,i)) ) % 256;
    //std::cout << finger << std::endl;

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
      finger_table[i] = 0;
    }
    //std::cout << finger_table[i] << std::endl;
  }
}
/*
int find_predecessor(const int node_id)
{
for(int i=0; i < Node::member_list.size(); i++)
{
if(node_id == Node::member_list[i])
break;
}
return i == 0 ? Node::member_list[Node::member_list.size()-1] : i-1 ;
}
*/
int find_successor(const int node_id)
{
  //std::cout << "Finding for " << node_id<< std::endl;
  int i=0;
  int n = Node::member_list.size();
  //std::cout << "Member list size ---- " << n <<std::endl;
  for(i=0; i < n; i++)
  {
    if(node_id == Node::member_list[i])
      break;
    }

    if(i+1 == n)
      return 0;
      else
        return Node::member_list[i+1];
      }

      void show_all()
    {
      int i=0, j=0;
      int n = Node::member_list.size();

      for(i =0 ; i < n; i++)
      {
        Node::messageQ[Node::member_list[i]].push("show-all");
      }

      while(listenerQ.size() != Node::member_list.size())
      {
        //wait for serialized key vectors from all nodes
      }

      std::sort(listenerQ.begin(), listenerQ.end());

      for(i = 0; i < listenerQ.size(); i++)
      {
        std::cout << "\nNode: " << listenerQ[i].first << ", Keys: " ;
        std::vector<int> data = MessageHandler::deserialize_vector(listenerQ[i].second);

        for(j=0; j < data.size(); j++)
        {
          std::cout << data[j] << " ";
        }

        std::cout << std::endl;
      }

      listenerQ.clear();
    }

    void merger(std::vector<int>& src, std::vector<int> dst)
  {
    std::vector<int> temp;
    temp.reserve(src.size() + dst.size()); // commenters are probably right about this
    std::merge(src.begin(), src.end(), dst.begin(), dst.end(), std::back_inserter(temp));
    src.swap(temp);
    std::sort(src.begin(), src.end());
  }

  void listener()
{
  // to store user input command
  int node_id, key, successor;
  std::string inputCommand, inputMessage, outputMessage;
  std::vector<std::string> inputMessageVector;
  std::vector<int>::iterator it;

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

    if(node_id >= 256)
    {
      std::cout << "\nInvalid Node ID: " << node_id << " Try again!" << std::endl;
      break;
    }

    std::cout << "\nNode: " << node_id << " is joining the system" << std::endl;

    update_member_list(node_id);
    Node::check_flag[node_id] = true;

    //std::cout << "And its flag is set" << std::endl;

    thread_pool[node_id] = std::thread(node_runner, node_id);
    display(Node::member_list);

    successor = find_successor(node_id);
    //std::cout << "Successor is " << successor<< std::endl;
    outputMessage = "send:" + std::to_string(node_id);
    //std::cout << outputMessage <<std::endl;
    push_message(successor, outputMessage);
    //std::cout <<"inserted" <<std::endl;
    break;

    case 2: //find
    node_id = std::stoi(inputMessageVector[1]);
    key = std::stoi(inputMessageVector[2]);
    it = find(Node::member_list.begin(), Node::member_list.end(), node_id);

    if( (node_id >= 256) || (it == Node::member_list.end()) )
    {
      std::cout << "\nInvalid Node ID: " << node_id << " Try again!" << std::endl;
      break;
    }
    std::cout << "\nFind key-->"<< key << " initiated at node-->" << node_id << std::endl;
    push_message(node_id, inputMessage);
    break;

    case 3: //leave
    node_id = std::stoi(inputMessageVector[1]);
    it = find(Node::member_list.begin(), Node::member_list.end(), node_id);

    if((node_id >= 256) || (it == Node::member_list.end()))
    {
      std::cout << "\nInvalid Node ID: " << node_id << " Try again!" << std::endl;
      break;
    }
    std::cout << "\nNode: " << node_id << " is leaving the system" << std::endl;
    push_message(node_id, inputMessage);
    //std::cout << "pushed message" << std::endl;
    break;

    case 4: //show
    node_id = std::stoi(inputMessageVector[1]);
    it = find(Node::member_list.begin(), Node::member_list.end(), node_id);

    if( (node_id > 256) || (it == Node::member_list.end()) )
    {
      std::cout << "\nNode: " << node_id << " does not exist. Try again!" << std::endl;
    }
    else
    {
      std::cout << "\nShow initiated at Node: " << node_id << std::endl;
      push_message(node_id, inputMessage);
    }

    break;

    case 5: //show-all
    std::cout << "\nShowing all keys"<< std::endl;
    show_all();
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

  int n,i,key;
  std::vector<int> temp;
  std::vector<int>::iterator it;

  std::string command, inputMessage, outputMessage;
  std::vector<std::string> inputMessageVector;

  // Initialize data once at Node 0
  if(node_id == 0)
  {
    for(int i=1; i<256; i++)
    {
      newnode.data.push_back(i);
    }
    newnode.data.push_back(0);
    std::cout << "\nOne time initialization data at Node 0: " <<newnode.data.size()<< std::endl;
  }

  while(Node::check_flag[node_id])
  {
    // when a new node joins, all other nodes should recompute their finger tables
    if(Node::redo_flag[node_id])
    {
      compute_finger_table(newnode.nodeID, newnode.finger_table);
      Node::redo_flag[node_id] = false;
    }

    if((newnode.nodeID == 0) && (newnode.data[0] == 0))
    {
      //std::cout << "W" << std::endl;
      newnode.data.erase(newnode.data.begin());
      newnode.data.push_back(0);
    }

    if(!Node::messageQ[node_id].empty())
    {
      inputMessage = Node::messageQ[node_id].front();
      //std::cout << "Node:" << node_id << " ,Message found--- " << inputMessage << std::endl;

      pop_message(node_id);

      inputMessageVector = MessageHandler::deserialize(inputMessage);
      command = inputMessageVector.front();

      switch(nodeCommandMap[command])
    {
      case 1: //send
      n = std::stoi(inputMessageVector[1]);
      //std::cout << "Size is " << temp.size() << std::endl;
      //display(newnode.data);

      while(newnode.data.front() <= n)
      {

        //std::cout << (newnode.data.front() == 0);
        if(newnode.data.front() == 0)
        {
          break;
        }
        temp.push_back(newnode.data.front());

        newnode.data.erase(newnode.data.begin());
      }

      outputMessage = "recv:" + MessageHandler::serialize_vector(temp);
      //std::cout << "\nMessage to be sent - " << outputMessage << std::endl;
      temp.clear();
      push_message(n, outputMessage);

      //std::cout << "Data from Node:" << node_id << " sent to " << n << std::endl;
      //std::cout << "Message Sent: " << outputMessage << std::endl;
      //std::cout << "Data suffled and sent to Node: " << n << std::endl;
      break;

      case 2: //find
      //std::cout << node_id << std::endl;
      key = std::stoi(inputMessageVector[2]);
      it = find(newnode.data.begin(), newnode.data.end(), key);
      if(it != newnode.data.end())
      {
        std::cout << "\nFound key: " << key << std::endl;
        std::cout << "Number of find messages exchanged ==> "  << Node::message_counter << std::endl;
        Node::message_counter = 0;
      }
      else
      {
        Node::message_counter += 1;

        int next_node = find_helper(key, newnode.finger_table);

        ostringstream ss;
        ss << "find " << next_node << " " << key;

        outputMessage = MessageHandler::serialize(ss.str());

        push_message(next_node, outputMessage);
      }
      break;

      case 3: //leave
      n = find_successor(node_id);
      //std::cout << "Size is " << temp.size() << std::endl;
      //display(newnode.data);

      outputMessage = "merge:" + MessageHandler::serialize_vector(newnode.data);
      //std::cout << outputMessage << std::endl;
      newnode.data.clear();

      push_message(n, outputMessage);

      Node::check_flag[node_id] = false;

      update_member_list(node_id, true);
      display(Node::member_list);
      break;

      case 4: //show
      std::cout << "\nShowing keys at Node:" << node_id << " Keys: ";
      for(int i = 0; i< newnode.data.size(); i++)
      {
        std::cout << newnode.data[i] << " ";
      }
      std::cout << std::endl;
      break;

      case 5: //show-all
      listenerQ.push_back(make_pair(node_id, MessageHandler::serialize_vector(newnode.data)));
      break;

      case 6: //recv
      //std::cout << "Node:" << node_id << " Message Received: " + inputMessage.substr(5) << std::endl;
      newnode.data = MessageHandler::deserialize_vector(inputMessage.substr(5));
      //display(newnode.data);
      break;

      case 7: //merge
      //std::cout << "Merging" <<std::endl;
      temp = MessageHandler::deserialize_vector(inputMessage.substr(6));
      merger(newnode.data, temp);
      //display(newnode.data);
      break;
    }

  }
}
}

/*
* Function - map commands
*/
void load_maps()
{
  commandMap["join"] = 1;
  commandMap["find"] = 2;
  commandMap["leave"] = 3;
  commandMap["show"] = 4;
  commandMap["show-all"] = 5;

  nodeCommandMap["send"] = 1;
  nodeCommandMap["find"] = 2;
  nodeCommandMap["leave"] = 3;
  nodeCommandMap["show"] = 4;
  nodeCommandMap["show-all"] = 5;
  nodeCommandMap["recv"] = 6;
  nodeCommandMap["merge"] = 7;

}

/*
* Function - Main function to map commands, spawn node 0 and spawn listener thread.
*/
int main(int argc, char *argv[])
{
  // display startup message
  std::cout << "\n=========================================" << std::endl;
  std::cout << "\n Intiliazing CHORD with Node 0 " << std::endl;
  std::cout << "\n=========================================\n" << std::endl;
  //Initially the system consists of node 0
  int node_id = 0;

  //Setting flags for node 0 and adding it to the member list
  Node::check_flag[node_id] = true;
  Node::redo_flag[node_id] = true;
  Node::member_list.push_back(node_id);

  //displaying the member list
  display(Node::member_list);

  //mapping the supported commands
  load_maps();

  // spawn the three threads
  std::thread coordinator_thread (listener);
  thread_pool[node_id] = std::thread(node_runner, node_id);

  // wait for all the node threads to complete their execution
  coordinator_thread.join();
  for(int i=0; i<32; i++)
  {
    thread_pool[i].join();
  }

  return 0;
}
