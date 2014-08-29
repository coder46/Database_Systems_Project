#include<iostream>
#include<string>
#include<fstream>
#include<sstream>
#include<vector>
#include<map>

using namespace std;

class DBSystem
{
	int psize,npages;
	string path;
	vector<string> pages;
	map<string,pair<int,int> > table_map;
	vector<pair<int,int> > page_map;
	map<int,int> mem_pages;
	public:
	string getRecord(string name,int id);
	string search(string page,int recordId);
	void readConfig(string filename);
	void populatepageInfo();
	void insertRecord(string tableName, string record);
	void flushPages();
};

void DBSystem::readConfig(string filename)
	{
		ifstream conf (filename.c_str(),ios::in);
		ofstream meta ("meta.txt",ios::out);
		string ll,s1,s2;
		int fl=0;
		if(conf.is_open())
		{
			while(getline(conf,ll))
			{
				stringstream line,temp;
				line << ll;
				line >> s1 >> s2;
				temp << s2;
				if(!s1.compare("PAGESIZE"))
					temp >> psize;
				else if(!s1.compare("NUM_PAGES"))
					temp >> npages;
				else if(!s1.compare("PATH_FOR_DATA"))
					temp >> path;
				else if(!s1.compare("BEGIN"))
					fl=1;
				else if(!s1.compare("END"))
				{
					fl=0;
					meta << '\n';
				}
				else
					meta << s1 << ' ';
			}
		}
		cout << psize << ' ' << npages << ' ' << path << '\n';
		conf.close();
		meta.close();
	}

void DBSystem::populatepageInfo()
{
		ifstream meta ("meta.txt");
		string ll;
		int page_num=0;
		while(getline(meta,ll))
		{
			pages.push_back("");
			stringstream line;
			string s1;
			int i,record_no=0,f_point=0;
			line << ll;
			line >> s1;
			ifstream file ((s1+".csv").c_str());
			string ll2;
			file.is_open();
			table_map[s1]=make_pair(page_num,page_num);
			page_map.push_back(make_pair(record_no,record_no));
			while(getline(file,ll2))
			{
				stringstream outpage,out2;
				string rec_header,rec;
				out2 << 0;
				rec_header.append(out2.str()+'#');
				for(i=0;i<ll2.length();i++)
				{
					if(ll2[i]==',')
					{
						stringstream out;
						out << i+1;
						rec_header.append(out.str()+'#');
					}
					else if(ll2[i]=='\n')
					{
					}
					else
					{
						stringstream out;
						out << ll2[i];
						rec.append(out.str());
					}

				}
				rec.append("$");
				stringstream out3;
				out3 << i << '#' << rec_header << rec;
				outpage  << out3.str().length() << '#' << rec_header << rec;
				if(outpage.str().length()+f_point > psize)
				{
					page_map[page_num].second=record_no-1;
					page_map.push_back(make_pair(record_no,record_no));

					page_num++;
					pages.push_back("");
					f_point=0;
				}
				record_no++;
				pages[page_num].append(outpage.str());
				f_point+=outpage.str().length();
			}
			f_point=0;
			page_map[page_num].second = record_no-1;
			record_no=0;
			table_map[s1].second=page_num;
			page_num++;
			page_map.push_back(make_pair(record_no,record_no));
			file.close();

		}
		meta.close();
		cout << "Here is page wise Printing(change the page size to get different results)" << '\n';

		for(int j=0;j<pages.size();j++)
			cout << pages[j] << '\n';
}

string DBSystem::getRecord(string tableName,int recordId)
{
	int startPage = table_map[tableName].first;
	int endPage = table_map[tableName].second;
	int pageReq = endPage;
	for(int i=startPage;i<endPage;i++)
		if(page_map[i].first <= recordId && page_map[i+1].first > recordId )
			pageReq = i;
	//cout << pageReq << "\n";
	map<int,int>::iterator it=mem_pages.find(pageReq);
	if(it!=mem_pages.end())
	{
		cout << "HIT" << '\n';
		mem_pages[pageReq]++;
	}
	else
	{
		cout << "MISS" << '\n';
		if(mem_pages.size()==npages)
		{
			map<int,int>::iterator least=mem_pages.begin();
			map<int,int>::iterator it=++least;
			least--;
			for(;it!=mem_pages.end();it++)
				if(it->second < least->second)
					least=it;
			mem_pages.erase(least->first);
			
		}
		mem_pages[pageReq]=1;

	}
	cout << search(pages[pageReq],recordId-page_map[pageReq].first) << "\n";

}

string DBSystem::search(string page,int recordId)
{
	int index=0;
	int length;
	string result;
	while(recordId!=0)
	{
		stringstream buf;
		buf << page.substr(index);
		buf >> length;
		index+=length;
		//cout << '\n' << page[index-1] << '\n' ;
		recordId--;

	}
	stringstream buf;
	buf << page.substr(index);
	buf >> length;
	//cout << "length :" << length << '\n';
	return page.substr(index,length);	
}

void DBSystem::insertRecord(string tableName, string record)
{

}

void DBSystem::flushPages()
{

}

int main()
{
	DBSystem mine;
	mine.readConfig("config.txt");
	mine.populatepageInfo();
	while(1)
	{
		stringstream query;
		string mystr,name;
		int id;
		getline(cin,mystr);
		query << mystr;
		query >> name >> id;
		cout << name << ' ' << id << '\n';
		mine.getRecord(name,id);
	}
	return 0;
}
