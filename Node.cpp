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

void node_runner(const int node_id, const int introducer);

// Function to pop safely from a node's message queue
void pop_message(const int node_id)
{
  mtx_locks[node_id].lock();
  Node::messageQ[node_id].pop();
  mtx_locks[node_id].unlock();
}

// Function to push safely to a node's message queue
void push_message(const int node_id, const std::string inputMessage)
{
  mtx_locks[node_id].lock();
  Node::messageQ[node_id].push(inputMessage);
  mtx_locks[node_id].unlock();
}

bool check_range(const int id, const int n1, const int n2)
{
  if(n1<n2)
  {
    return (n1<=id && id<=n2);
  }
  if(n1>n2)
  {
    return !(n2<id && id<n1);
  }
  return n1 == id;
}
/*
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
*/

void display_ft(const std::array<std::pair<int,int>,8>& v)
{
  std::cout << "finger table here we go " <<std::endl;
  for(int i=0;i<8;i++)
  {
    std::cout << i << "--" << v[i].first << "," << v[i].second <<std::endl;
  }
}
// Function to display a vector
void display(const std::vector<int>& v)
{
  std::cout << "Membership List: ";
  for (int i=0; i<v.size();i++)
  {
    cout << v[i] << " ";
  }
  std::cout << std::endl;
}

// Function to keep track of the active nodes - if needed
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

  /*
  //Set flag so that all nodes in the system recompute its finger table
  for (int i=0; i < Node::member_list.size(); i++)
  {
    Node::redo_flag[Node::member_list[i]] = true;
  }
  */
  //std::cout << "in update" << std::endl;
}
/*
void compute_finger_table(const int node_id, std::array<std::pair<int,int>,8>& finger_table)
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

// Function to help find the predecessor in a ring
int find_p(const int node_id)
{
  int i;
  for(i=0; i < Node::member_list.size(); i++)
  {
    if(node_id == Node::member_list[i])
      break;
  }
  return i == 0 ? Node::member_list[Node::member_list.size()-1] : Node::member_list[i-1] ;
}

// Function to help find the successor in a ring
/*
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

  return Node::member_list[(i+1)%n];
}*/

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

int Node::find_successor(const int id)
{
  //std::cout << "Inside Node find successor of " << this->nodeID << std::endl;
  int node_id = this->nodeID;
  ostringstream ss;
  std::string inputMessage, outputMessag;
  std::vector<std::string> inputMessageVector;

  int n = this->find_predecessor(id);

  ss << "get_succ " << node_id;
  inputMessage = MessageHandler::serialize(ss.str());

  if(n == this->nodeID)
  {
    return this->finger_table[0].second;
  }

  Node::message_counter += 1;

  push_message(n, inputMessage);

  while(Node::messageQ[node_id].empty())
  {
    //Wait for message
  }

  //std::cout << "find pred is done " << n << " " << Node::messageQ[node_id].front() << std::endl;
  // pop the returned value
  inputMessageVector = MessageHandler::deserialize(Node::messageQ[node_id].front());
  pop_message(node_id);

  return std::stoi(inputMessageVector[1]);

}

int Node::find_predecessor(const int id)
{
  ostringstream ss;
  std::string inputMessage, outputMessag;
  std::vector<std::string> inputMessageVector;
  int n1 = this->nodeID;
  int n2 = this->finger_table[0].second;
  //std::cout << "Inside Node predecessor" << id << n1 << n2 << std::endl;
  while( !(check_range(id, n1+1, n2))  )
  {

    //std::cout << "Inside Node predecessor" << id << n1 << n2 << std::endl;
    if(n1==this->nodeID)
    {
      n1 = this->find_closest_preceding_finger(id);
    }
    else
    {
      ss.str("");
      ss.clear();

      ss << "run_cpf " << id << " " << this->nodeID;
      inputMessage = MessageHandler::serialize(ss.str());

      Node::message_counter += 1;

      push_message(n1, inputMessage);


      while(Node::messageQ[this->nodeID].empty())
      {
        //Wait for message
      }

      //std::cout << "I am here " << Node::messageQ[this->nodeID].front() << std::endl;
      inputMessageVector = MessageHandler::deserialize(Node::messageQ[this->nodeID].front());
      // pop the returned value
      pop_message(this->nodeID);
      n1 = std::stoi(inputMessageVector[1]);
    }
    //ask n1 to do cpf and return the cpf

    // n1 = set to that cpf

    // this n1 will seek its successor

    ss.str("");
    ss.clear();


    ss << "get_succ " << this->nodeID;
    inputMessage = MessageHandler::serialize(ss.str());

    Node::message_counter += 1;

    push_message(n1, inputMessage);


    while(Node::messageQ[this->nodeID].empty())
    {
      //Wait for message
    }

    //std::cout << "I am here " << Node::messageQ[this->nodeID].front() << std::endl;
    inputMessageVector = MessageHandler::deserialize(Node::messageQ[this->nodeID].front());
    // pop the returned value
    pop_message(this->nodeID);
    n2 = std::stoi(inputMessageVector[1]);
  }

  return n1;
}

int Node::find_closest_preceding_finger(int id)
{
  //std::cout << "Inside Node find closest preceding finger " << this->nodeID << std::endl;
  int n1 = this->nodeID;
  //if(n1==8) exit(0);
  int n2 = id-1;
  int key;

  for(int i=7; i>=0; i--)
  {
    key = this->finger_table[i].second;

    if( check_range(key,n1+1,n2) )
    {
      return key;
    }
  }

  //need some way to get the node identified by int
  return this->nodeID;
}

void Node::init()
{
  //std::cout << "Inside Node init" << std::endl;
  std::pair<int,int> temp;

  int node_id = this->nodeID;

  for(int i=0; i<8; i++)
  {
    temp.first = (node_id + static_cast<int>(pow(2,i)) ) % 256;
    temp.second = 0;
    this->finger_table[i] = temp;
  }
}

int Node::init_ft(const int introducer)
{
  //std::cout << "Inside Node init finger table" << std::endl;
  int key, n2, n1, node_id = this->nodeID;
  std::string inputMessage, outputMessag;
  std::vector<std::string> inputMessageVector;
  ostringstream ss;

  ss << "find_succ " << node_id << " " << this->finger_table[0].first;

  inputMessage = MessageHandler::serialize(ss.str());

  // first message to introducer to find the successor
  Node::message_counter += 1;
  //std::cout << "in inti_ft pushed - " << inputMessage << std::endl;
  // push message to the introduer's queue
  push_message(introducer, inputMessage);

  while(Node::messageQ[node_id].empty())
  {
    //Wait for message
  }

  // pop the returned value
  //std::cout << "in inti_ft popped- " << Node::messageQ[node_id].front() << std::endl;
  inputMessageVector = MessageHandler::deserialize(Node::messageQ[node_id].front());
  this->finger_table[0].second = std::stoi(inputMessageVector[1]);
  pop_message(node_id);

  //Node::message_counte += 1;
  this->predecessor = std::stoi(inputMessageVector[2]);
  //std::cout << "setting pred- " << this->predecessor << std::endl;

  ss.str("");
  ss.clear();

  ss << "put_pred " << node_id;
  inputMessage = MessageHandler::serialize(ss.str());

  // second message to update the successor's predecessor
  Node::message_counter += 1;

  // push message to the successor's queue
  push_message(this->finger_table[0].second, inputMessage);

  //wait for reply
  while(Node::messageQ[node_id].empty())
  {
    //Wait for message
  }
  pop_message(node_id);

  // for the other entries
  for(int i=0; i<7; i++)
  {
    key = this->finger_table[i+1].first;
    n1 = this->nodeID;
    n2 = this->finger_table[i].second;
    //std::cout << "in inti_ft for - " << n2 << "and" << i << std::endl;

    if( check_range(key, n1, n2-1) )
    {
      this->finger_table[i+1].second = this->finger_table[i].second;
    }
    else
    {
      ss.str("");
      ss.clear();

      ss << "find_succ " << node_id << " " << this->finger_table[i+1].first;

      inputMessage = MessageHandler::serialize(ss.str());;

      // message to introducer to find the successor
      Node::message_counter += 1;
      //std::cout << "in inti_ft pushed - " << inputMessage << std::endl;
      // push message to the introduer's queue
      push_message(introducer, inputMessage);

      while(Node::messageQ[node_id].empty())
      {
        //Wait for message from introducer
      }
      //std::cout << "in inti_ft popped- " << Node::messageQ[node_id].front() << std::endl;
      // pop the returned value
      inputMessageVector = MessageHandler::deserialize(Node::messageQ[node_id].front());
      this->finger_table[i+1].second = std::stoi(inputMessageVector[1]);
      pop_message(node_id);

      key = this->finger_table[i+1].second;
      n1 = this->finger_table[i+1].first;
      n2 = this->nodeID;
      if( ! (check_range(key, n1, n2) ) )
      {
        this->finger_table[i+1].second = this->nodeID;
      }

    }


  }

}

void Node::join(const int introducer)
{
  //std::cout << "Inside Node join" << std::endl;
  this->init_ft(introducer);
  //std::cout << "Init FT has compeleted, now we being update others" << std::endl;
  //display_ft(this->finger_table);
  this->update_others();
}

void Node::update_others()
{
  int node_id = this->nodeID;
  //std::cout << "Update others in " << node_id << std::endl;
  int p, omega;
  std::string inputMessage;
  ostringstream ss;

  for(int i=0; i<8; i++)
  {
    ss.str("");
    ss.clear();

    omega = ( ( (this->nodeID - static_cast<int>(pow(2,i)) + 1) % 256) + 256 ) % 256 ;
    p = this->find_predecessor(omega);
    //std::cout << "Found pred as " << p  << " and id is--->>>>>>   " << omega << std::endl;
    ss << "update_ft " << this->nodeID << " " << i << " " << this->nodeID;

    /*if(p == this->nodeID)
    {
      continue;
    }*/
    inputMessage = MessageHandler::serialize(ss.str());;

    // first message to introducer to find the successor
    Node::message_counter += 1;

    // push message to the introduer's queue
    push_message(p, inputMessage);

    while(Node::messageQ[node_id].empty())
    {
      //Wait for message
    }
    pop_message(node_id);
  }
}

void Node::update_ft(const int s, const int i)
{
  if(s == this->nodeID)
  {
    return;
  }
  int p, node_id = this->nodeID;
  int n1 = this->nodeID;
  int n2 = this->finger_table[i].second;
  ostringstream ss;
  std::string inputMessage;

  //std::cout << "Inside Update ft - " << s << n1 << n2 << std::endl;

  if( check_range(s, n1+1, n2))
  {
    //std::cout << "updating ft " << this->nodeID << std::endl;
    this->finger_table[i].second = s;

    // get first node preceeding n
    p = this->predecessor;
    if(p==s)
      return;
    ss << "update_ft " << s << " " << i << " " << this->nodeID;

    inputMessage = MessageHandler::serialize(ss.str());;

    // first message to introducer to find the successor
    Node::message_counter += 1;

    // push message to the introduer's queue
    push_message(p, inputMessage);

    while(Node::messageQ[node_id].empty())
    {
      //Wait for message
    }
    pop_message(node_id);
  }
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
        Node::message_counter = 0;
        node_id = std::stoi(inputMessageVector[1]);

        if(node_id >= 256)
        {
          std::cout << "\nInvalid Node ID: " << node_id << " Try again!" << std::endl;
          break;
        }

        std::cout << "\nNode: " << node_id << " is joining the system" << std::endl;

        // Just to have the Listener keep track of the active nodes
        update_member_list(node_id);
        // Set Node thread's flag so that it can become active
        Node::check_flag[node_id] = true;

        //std::cout << "And its flag is set" << std::endl;

        thread_pool[node_id] = std::thread(node_runner, node_id, 0);
        display(Node::member_list);
        break;

      case 2: //find
        node_id = std::stoi(inputMessageVector[1]);
        key = std::stoi(inputMessageVector[2]);
        it = find(Node::member_list.begin(), Node::member_list.end(), node_id);

        if( (node_id >= 256) || (it == Node::member_list.end()) )
        {
          std::cout << "\nInvalid Node ID or key: " << node_id << " Try again!" << std::endl;
          break;
        }
        Node::message_counter = 0;
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

      case 6: //display finger table
        node_id = std::stoi(inputMessageVector[1]);
        std::cout << "\nDisplay FT initiated at node-->" << node_id << std::endl;
        push_message(node_id, inputMessage);
        break;

      default:
        std::cout << "\nPlease check your input" << std::endl;
        break;
    }
  }
}

void node_runner(const int node_id, const int introducer)
{
  //std::cout << "Hi, I am node: " << node_id << std::endl;
  Node newnode;
  newnode.nodeID = node_id;

  int n,i,key;
  ostringstream ss;
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

  newnode.init();
  if(node_id != 0)
  {
    newnode.join(introducer);
    //std::cout << "wooohoooooo"  << std::endl;
    std::queue<std::string> empty;
    std::swap( Node::messageQ[node_id], empty );
    //successor = find_successor(node_id);
    //std::cout << "Successor is " << newnode.finger_table[0].second << std::endl;
    outputMessage = "send:" + std::to_string(newnode.nodeID);
    //std::cout << outputMessage << " and queue size is " << Node::messageQ[node_id].size() << std::endl;
    Node::message_counter += 1;
    push_message(newnode.finger_table[0].second, outputMessage);
    //std::cout <<"inserted" <<std::endl;

    while(Node::messageQ[node_id].empty())
    {
      //Wait for message
    }

    inputMessage = Node::messageQ[node_id].front();
    //std::cout << "Node:" << node_id << " Message Received: " + inputMessage << std::endl;
  //std::cout << "i am here: " << Node::messageQ[node_id].front() << std::endl;
    newnode.data = MessageHandler::deserialize_vector(inputMessage.substr(5));
    pop_message(node_id);
  //exit(0);
    //display(newnode.data);
    //exit(0);

    std::cout << "Number of join messages exchanged ==> "  << Node::message_counter << std::endl;
    Node::message_counter = 0;
  }

  while(Node::check_flag[node_id])
  {
    /* when a new node joins, all other nodes should recompute their finger tables
    if(Node::redo_flag[node_id])
    {
      compute_finger_table(newnode.nodeID, newnode.finger_table);
      Node::redo_flag[node_id] = false;
    }*/
    //std::cout << "Node id: " << node_id << " predecessor is " << newnode.predecessor << std::endl;
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

          //std::cout << "Size is " << temp.size() << " and sending to " << inputMessageVector[1] << std::endl;
          n = std::stoi(inputMessageVector[1]);
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
          //display(newnode.data);
          //exit(0);
          //std::cout << "\nMessage to be sent to -" << n <<" - " << outputMessage << std::endl;
          temp.clear();
          Node::message_counter += 1;
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
            n = newnode.find_successor(key);

            ss.str("");
            ss.clear();

            ss << "find " << n << " " << key;
            inputMessage = MessageHandler::serialize(ss.str());

            Node::message_counter += 1;

            // push message to the successor's queue
            push_message(n, inputMessage);
          }
          break;

        case 3: //leave
  //        n = find_successor(node_id);
          //std::cout << "Size is " << temp.size() << std::endl;
          //display(newnode.data);

          outputMessage = "merge:" + MessageHandler::serialize_vector(newnode.data);
          //std::cout << outputMessage << std::endl;
          newnode.data.clear();

          push_message(newnode.finger_table[0].second, outputMessage);

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
          //std::cout << "\nPushing keys at Node:" << node_id << " Keys: ";
          listenerQ.push_back(make_pair(newnode.nodeID, MessageHandler::serialize_vector(newnode.data)));
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

        case 8: //find successor
          //std::cout << "Find successor: " << inputMessageVector[1] << " "<< inputMessageVector[2] << std::endl;
          ss.str("");
          ss.clear();

          n = std::stoi(inputMessageVector[1]);
          key = std::stoi(inputMessageVector[2]);

          ss << "succ " << newnode.find_successor(key) << " " << newnode.predecessor;
          outputMessage = MessageHandler::serialize(ss.str());
          Node::message_counter += 1;

          // push message to the introduer's queue
          push_message(n, outputMessage);
          //display(newnode.data);
          break;

        case 9: // put predecessor
          //std::cout << "Merging" <<std::endl;
          n = std::stoi(inputMessageVector[1]);
          newnode.predecessor = n;
          outputMessage = MessageHandler::serialize("Done");
          Node::message_counter += 1;

          // push message to the introduer's queue
          push_message(n, inputMessage);
          //display(newnode.data);
          break;


        case 10: //update finger table entry
          //std::cout << "Merging" <<std::endl;

          n = std::stoi(inputMessageVector[1]);
          key = std::stoi(inputMessageVector[2]);

          newnode.update_ft(n,key);

          outputMessage = MessageHandler::serialize("ACK");
          Node::message_counter += 1;

          // push message to the introduer's queue
          push_message(std::stoi(inputMessageVector[3]), outputMessage);
          //display(newnode.data);
          break;

        case 11: // get the successor
          //std::cout << "get successor" << std::endl;
          ss.str("");
          ss.clear();

          n = std::stoi(inputMessageVector[1]);
          ss << "succ " << newnode.finger_table[0].second;
          outputMessage = MessageHandler::serialize(ss.str());
          Node::message_counter += 1;

          // push message to the introduer's queue
          push_message(n, outputMessage);
          break;

        case 12:
          display_ft(newnode.finger_table);
          break;

        case 13: // run cpf remote
          //std::cout << "running cpf" << std::endl;
          ss.str("");
          ss.clear();

          key = std::stoi(inputMessageVector[1]);
          n = std::stoi(inputMessageVector[2]);

          ss << "cpf " << newnode.find_closest_preceding_finger(key);
          outputMessage = MessageHandler::serialize(ss.str());
          Node::message_counter += 1;

          // push message to the introduer's queue
          push_message(n, outputMessage);
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
  commandMap["display"] = 6;

  nodeCommandMap["send"] = 1;
  nodeCommandMap["find"] = 2;
  nodeCommandMap["leave"] = 3;
  nodeCommandMap["show"] = 4;
  nodeCommandMap["show-all"] = 5;
  nodeCommandMap["recv"] = 6;
  nodeCommandMap["merge"] = 7;
  nodeCommandMap["find_succ"] = 8;
  nodeCommandMap["put_pred"] = 9;
  nodeCommandMap["update_ft"] = 10;
  nodeCommandMap["get_succ"] = 11;
  nodeCommandMap["display"] = 12;
  nodeCommandMap["run_cpf"] = 13;

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
  thread_pool[node_id] = std::thread(node_runner, node_id, 0);

  // wait for all the node threads to complete their execution
  coordinator_thread.join();
  for(int i=0; i<32; i++)
  {
    thread_pool[i].join();
  }

  return 0;
}
