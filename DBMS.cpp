#include<iostream>
#include<cstdio>
#include<vector>
#include<string>

using namespace std;

class DBSystem
{
	public:
		void readConfig(string configFilePath);
		void populateDBInfo();
		string getRecord(string tableName, int recordId);
		void insertRecord(string tableName, string record);
		void flushPages();
};

void DBSystem::readConfig(string configFilePath)
{

}

void DBSystem::populateDBInfo()
{

}

string DBSystem::getRecord(string tableName, int recordId)
{

}

void DBSystem::insertRecord(string tableName, string record)
{

}

void DBSystem::flushPages()
{

}

int main()
{

}


